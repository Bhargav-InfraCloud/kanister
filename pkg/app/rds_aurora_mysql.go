// Copyright 2021 The Kanister Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package app

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	awssdk "github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	rdserr "github.com/aws/aws-sdk-go/service/rds"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	crv1alpha1 "github.com/kanisterio/kanister/pkg/apis/cr/v1alpha1"
	aws "github.com/kanisterio/kanister/pkg/aws"
	"github.com/kanisterio/kanister/pkg/aws/ec2"
	"github.com/kanisterio/kanister/pkg/aws/rds"
	"github.com/kanisterio/kanister/pkg/field"
	"github.com/kanisterio/kanister/pkg/function"
	"github.com/kanisterio/kanister/pkg/kube"
	"github.com/kanisterio/kanister/pkg/log"
	"github.com/kanisterio/kanister/pkg/poll"

	_ "github.com/go-sql-driver/mysql"
)

const (
	AuroraDBInstanceClass = "db.r5.large"
	AuroraDBStorage       = 20
	DetailsCMName         = "dbconfig"
)

type RDSAuroraMySQLDB struct {
	name              string
	cli               kubernetes.Interface
	namespace         string
	id                string
	host              string
	dbName            string
	username          string
	password          string
	accessID          string
	secretKey         string
	region            string
	sessionToken      string
	securityGroupID   string
	securityGroupName string
}

func NewRDSAuroraMySQLDB(name, region string) App {
	return &RDSAuroraMySQLDB{
		name:              name,
		id:                fmt.Sprintf("test-%s", name),
		securityGroupName: fmt.Sprintf("%s-sg", name),
		region:            region,
		username:          "admin",
		password:          "secret99",
		dbName:            "testdb",
	}
}

func (a *RDSAuroraMySQLDB) Init(context.Context) error {
	cfg, err := kube.LoadConfig()
	if err != nil {
		return err
	}

	var ok bool
	a.cli, err = kubernetes.NewForConfig(cfg)
	if err != nil {
		return err
	}
	if a.region == "" {
		a.region, ok = os.LookupEnv(aws.Region)
		if !ok {
			return fmt.Errorf("env var %s is not set", aws.Region)
		}
	}

	// If sessionToken is set, accessID and secretKey not required
	a.sessionToken, ok = os.LookupEnv(aws.SessionToken)
	if ok {
		return nil
	}

	a.accessID, ok = os.LookupEnv(aws.AccessKeyID)
	if !ok {
		return fmt.Errorf("env var %s is not set", aws.AccessKeyID)
	}
	a.secretKey, ok = os.LookupEnv(aws.SecretAccessKey)
	if !ok {
		return fmt.Errorf("env var %s is not set", aws.SecretAccessKey)
	}

	return nil
}

func (a *RDSAuroraMySQLDB) Install(ctx context.Context, namespace string) error {
	a.namespace = namespace

	// Get aws config
	awsConfig, region, err := a.getAWSConfig(ctx)
	if err != nil {
		return fmt.Errorf("error getting aws config app=%s: %w", a.name, err)
	}

	// Create ec2 client
	ec2Cli, err := ec2.NewClient(ctx, awsConfig, region)
	if err != nil {
		return err
	}

	// Create security group
	log.Info().Print("Creating security group.", field.M{"app": a.name, "name": a.securityGroupName})
	sg, err := ec2Cli.CreateSecurityGroup(ctx, a.securityGroupName, "To allow ingress to Aurora DB cluster")
	if err != nil {
		return fmt.Errorf("error creating security group: %w", err)
	}
	a.securityGroupID = *sg.GroupId

	// Add ingress rule
	_, err = ec2Cli.AuthorizeSecurityGroupIngress(ctx, a.securityGroupName, "0.0.0.0/0", "tcp", 3306)
	if err != nil {
		return fmt.Errorf("error authorizing security group: %w", err)
	}

	rdsCli, err := rds.NewClient(ctx, awsConfig, region)
	if err != nil {
		return err
	}

	// Create RDS instance
	log.Info().Print("Creating RDS Aurora DB cluster.", field.M{"app": a.name, "id": a.id})
	_, err = rdsCli.CreateDBCluster(ctx, AuroraDBStorage, AuroraDBInstanceClass, a.id, string(function.DBEngineAuroraMySQL), a.dbName, a.username, a.password, []string{a.securityGroupID})
	if err != nil {
		return fmt.Errorf("error creating DB cluster: %w", err)
	}

	err = rdsCli.WaitUntilDBClusterAvailable(ctx, a.id)
	if err != nil {
		return fmt.Errorf("error waiting for DB cluster to be available: %w", err)
	}

	// create db instance in the cluster
	_, err = rdsCli.CreateDBInstanceInCluster(ctx, a.id, fmt.Sprintf("%s-instance-1", a.id), AuroraDBInstanceClass, string(function.DBEngineAuroraMySQL))
	if err != nil {
		return fmt.Errorf("error creating an instance in Aurora DB cluster: %w", err)
	}

	err = rdsCli.WaitUntilDBInstanceAvailable(ctx, fmt.Sprintf("%s-instance-1", a.id))
	if err != nil {
		return fmt.Errorf("error waiting for DB instance to be available: %w", err)
	}

	dbCluster, err := rdsCli.DescribeDBClusters(ctx, a.id)
	if err != nil {
		return err
	}
	if len(dbCluster.DBClusters) == 0 {
		return fmt.Errorf("error installing application %s, DBCluster not available", a.name)
	}
	a.host = *dbCluster.DBClusters[0].Endpoint

	// Configmap that is going to store the details for blueprint
	cm := &v1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ConfigMap",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: DetailsCMName,
		},
		Data: map[string]string{
			"aurora.clusterID": a.id,
		},
	}

	_, err = a.cli.CoreV1().ConfigMaps(namespace).Create(ctx, cm, metav1.CreateOptions{})
	return err
}

func (a *RDSAuroraMySQLDB) IsReady(context.Context) (bool, error) {
	// we are already waiting for dbcluster using WaitUntilDBClusterAvailable while installing it
	return true, nil
}

func (a *RDSAuroraMySQLDB) Ping(context.Context) error {
	db, err := a.openDBConnection()
	if err != nil {
		return fmt.Errorf("error opening database connection: %w", err)
	}
	defer func() {
		if err = a.closeDBConnection(db); err != nil {
			log.Print("Error closing DB connection", field.M{"app": a.name})
		}
	}()

	pingQuery := "select 1"
	_, err = db.Query(pingQuery)
	return err
}

func (a *RDSAuroraMySQLDB) Insert(ctx context.Context) error {
	db, err := a.openDBConnection()
	if err != nil {
		return err
	}
	defer func() {
		if err = a.closeDBConnection(db); err != nil {
			log.Print("Error closing DB connection", field.M{"app": a.name})
		}
	}()

	query, err := db.Prepare("INSERT INTO pets VALUES (?,?,?,?,?,?);")
	if err != nil {
		return fmt.Errorf("error preparing query: %w", err)
	}

	// start a transaction
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("error beginning transaction: %w", err)
	}

	_, err = tx.Stmt(query).Exec("Puffball", "Diane", "hamster", "f", "1999-03-30", "NULL")
	if err != nil {
		return fmt.Errorf("error inserting data into Aurora DB cluster: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("error committing data into Aurora DB database: %w", err)
	}

	return nil
}

func (a *RDSAuroraMySQLDB) Count(context.Context) (int, error) {
	db, err := a.openDBConnection()
	if err != nil {
		return 0, err
	}
	defer func() {
		if err = a.closeDBConnection(db); err != nil {
			log.Print("Error closing DB connection", field.M{"app": a.name})
		}
	}()

	rows, err := db.Query("select * from pets;")
	if err != nil {
		return 0, fmt.Errorf("error preparing count query: %w", err)
	}
	count := 0
	for rows.Next() {
		count++
	}

	return count, nil
}

func (a *RDSAuroraMySQLDB) Reset(ctx context.Context) error {
	timeoutCtx, waitCancel := context.WithTimeout(ctx, mysqlWaitTimeout)
	defer waitCancel()
	err := poll.Wait(timeoutCtx, func(ctx context.Context) (bool, error) {
		err := a.Ping(ctx)
		return err == nil, nil
	})

	if err != nil {
		return fmt.Errorf("error waiting for application %s to be ready to reset it: %w", a.name, err)
	}

	log.Print("Resetting the mysql instance.", field.M{"app": a.name})

	db, err := a.openDBConnection()
	if err != nil {
		return err
	}
	defer func() {
		if err = a.closeDBConnection(db); err != nil {
			log.Print("Error closing DB connection", field.M{"app": a.name})
		}
	}()

	query, err := db.Prepare("DROP TABLE IF EXISTS pets;")
	if err != nil {
		return fmt.Errorf("error preparing reset query: %w", err)
	}

	// start a transaction
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("error beginning transaction: %w", err)
	}

	_, err = tx.Stmt(query).Exec()
	if err != nil {
		return fmt.Errorf("error resetting Aurora database: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("error committing DB reset transaction: %w", err)
	}

	return nil
}

func (a *RDSAuroraMySQLDB) Initialize(context.Context) error {
	db, err := a.openDBConnection()
	if err != nil {
		return err
	}

	defer func() {
		if err = a.closeDBConnection(db); err != nil {
			log.Print("Error closing DB connection", field.M{"app": a.name})
		}
	}()

	query, err := db.Prepare("CREATE TABLE pets (name VARCHAR(20), owner VARCHAR(20), species VARCHAR(20), sex CHAR(1), birth DATE, death DATE);")
	if err != nil {
		return fmt.Errorf("error preparing query: %w", err)
	}

	// start a transaction
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("error begining transaction: %w", err)
	}

	_, err = tx.Stmt(query).Exec()
	if err != nil {
		return fmt.Errorf("error creating table into Aurora database: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("error committing table creation: %w", err)
	}
	return nil
}

func (a *RDSAuroraMySQLDB) Object() crv1alpha1.ObjectReference {
	return crv1alpha1.ObjectReference{
		APIVersion: "v1",
		Name:       DetailsCMName,
		Namespace:  a.namespace,
		Resource:   "configmaps",
	}
}

func (a *RDSAuroraMySQLDB) Uninstall(ctx context.Context) error {
	awsConfig, region, err := a.getAWSConfig(ctx)
	if err != nil {
		return fmt.Errorf("error app=%s: %w", a.name, err)
	}
	// Create rds client
	rdsCli, err := rds.NewClient(ctx, awsConfig, region)
	if err != nil {
		return fmt.Errorf("failed to create rds client. You may need to delete RDS resources manually. app=rds-postgresql: %w", err)
	}

	descOp, err := rdsCli.DescribeDBClusters(ctx, a.id)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			if aerr.Code() != rdserr.ErrCodeDBClusterNotFoundFault {
				return err
			}
			log.Print("Aurora DB cluster is not found")
		}
	} else {
		// DB Cluster is present, delete and wait for it to be deleted
		if err := function.DeleteAuroraDBCluster(ctx, rdsCli, descOp, a.id); err != nil {
			return nil
		}
	}

	// Create ec2 client
	ec2Cli, err := ec2.NewClient(ctx, awsConfig, region)
	if err != nil {
		return fmt.Errorf("failed to create ec2 client: %w", err)
	}

	// delete security group
	log.Info().Print("Deleting security group.", field.M{"app": a.name})
	_, err = ec2Cli.DeleteSecurityGroup(ctx, a.securityGroupName)
	if err != nil {
		if err, ok := err.(awserr.Error); ok {
			switch err.Code() {
			case "InvalidGroup.NotFound":
				log.Error().Print("Security group already deleted: InvalidGroup.NotFound.", field.M{"app": a.name, "name": a.securityGroupName})
			default:
				return fmt.Errorf("failed to delete security group. You may need to delete it manually. app=rds-postgresql name=%s: %w", a.securityGroupName, err)
			}
		}
	}

	return nil
}

func (a *RDSAuroraMySQLDB) GetClusterScopedResources(ctx context.Context) []crv1alpha1.ObjectReference {
	return nil
}

func (a *RDSAuroraMySQLDB) getAWSConfig(ctx context.Context) (*awssdk.Config, string, error) {
	config := make(map[string]string)
	config[aws.ConfigRegion] = a.region
	config[aws.AccessKeyID] = a.accessID
	config[aws.SecretAccessKey] = a.secretKey
	config[aws.SessionToken] = a.sessionToken
	return aws.GetConfig(ctx, config)
}

func (a *RDSAuroraMySQLDB) openDBConnection() (*sql.DB, error) {
	return sql.Open("mysql", fmt.Sprintf("%s:%s@(%s)/%s", a.username, a.password, a.host, a.dbName))
}

func (a RDSAuroraMySQLDB) closeDBConnection(db *sql.DB) error {
	return db.Close()
}
