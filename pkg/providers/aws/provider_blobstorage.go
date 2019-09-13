package aws

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/aws/aws-sdk-go/aws/awserr"

	"k8s.io/apimachinery/pkg/util/wait"

	v1 "github.com/openshift/cloud-credential-operator/pkg/apis/cloudcredential/v1"
	controllerruntime "sigs.k8s.io/controller-runtime"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/integr8ly/cloud-resource-operator/pkg/resources"

	"github.com/integr8ly/cloud-resource-operator/pkg/providers"

	"github.com/aws/aws-sdk-go/aws"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/integr8ly/cloud-resource-operator/pkg/apis/integreatly/v1alpha1"
	errorUtil "github.com/pkg/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	dataBucketName          = "bucketName"
	dataCredentialKeyID     = "credentialKeyID"
	dataCredentialSecretKey = "credentialSecretKey"
)

// BlobStorageDeploymentDetails Provider-specific details about the AWS S3 bucket created
type BlobStorageDeploymentDetails struct {
	BucketName          string
	CredentialKeyID     string
	CredentialSecretKey string
}

func (d *BlobStorageDeploymentDetails) Data() map[string][]byte {
	return map[string][]byte{
		dataBucketName:          []byte(d.BucketName),
		dataCredentialKeyID:     []byte(d.CredentialKeyID),
		dataCredentialSecretKey: []byte(d.CredentialSecretKey),
	}
}

var _ providers.BlobStorageProvider = (*BlobStorageProvider)(nil)

// BlobStorageProvider BlobStorageProvider implementation for AWS S3
type BlobStorageProvider struct {
	Client            client.Client
	Logger            *logrus.Entry
	CredentialManager CredentialManager
	ConfigManager     ConfigManager
}

func NewAWSBlobStorageProvider(client client.Client, logger *logrus.Entry) *BlobStorageProvider {
	return &BlobStorageProvider{
		Client:            client,
		Logger:            logger.WithFields(logrus.Fields{"provider": "aws_s3"}),
		CredentialManager: NewCredentialMinterCredentialManager(client),
		ConfigManager:     NewDefaultConfigMapConfigManager(client),
	}
}

func (p *BlobStorageProvider) GetName() string {
	return providers.AWSDeploymentStrategy
}

func (p *BlobStorageProvider) SupportsStrategy(d string) bool {
	return d == providers.AWSDeploymentStrategy
}

// CreateStorage Create S3 bucket from strategy config and credentials to interact with it
func (p *BlobStorageProvider) CreateStorage(ctx context.Context, bs *v1alpha1.BlobStorage) (*providers.BlobStorageInstance, error) {
	p.Logger.Infof("creating blob storage instance %s via aws s3", bs.Name)
	// handle provider-specific finalizer
	p.Logger.Infof("adding finalizer to blob storage instance %s", bs.Name)
	if bs.GetDeletionTimestamp() == nil {
		resources.AddFinalizer(&bs.ObjectMeta, DefaultFinalizer)
		if err := p.Client.Update(ctx, bs); err != nil {
			return nil, errorUtil.Wrapf(err, "failed to add finalizer to blob storage instance %s", bs.Name)
		}
	}

	// info about the bucket to be created
	p.Logger.Infof("getting aws s3 bucket config for blob storage instance %s", bs.Name)
	bucketCreateCfg, stratCfg, err := p.getS3BucketConfig(ctx, bs)
	if err != nil {
		return nil, errorUtil.Wrapf(err, "failed to retrieve aws s3 bucket config for blob storage instance %s", bs.Name)
	}
	if bucketCreateCfg.Bucket == nil {
		bucketCreateCfg.Bucket = aws.String(fmt.Sprintf("%s-%s", bs.Namespace, bs.Name))
	}

	// create the credentials to be used by the end-user, whoever created the blobstorage instance
	endUserCredsName := fmt.Sprintf("cloud-resources-aws-s3-%s-credentials", bs.Name)
	p.Logger.Infof("creating end-user credentials with name %s for managing s3 bucket %s", endUserCredsName, *bucketCreateCfg.Bucket)
	endUserCreds, _, err := p.CredentialManager.ReoncileBucketOwnerCredentials(ctx, endUserCredsName, bs.Namespace, *bucketCreateCfg.Bucket)
	if err != nil {
		return nil, errorUtil.Wrapf(err, "failed to reconcile s3 end-user credentials for blob storage instance %s", bs.Name)
	}

	// create the credentials to be used by the aws resource providers, not to be used by end-user
	p.Logger.Infof("creating provider credentials for creating s3 buckets, in namespace %s", bs.Namespace)
	providerCreds, err := p.CredentialManager.ReconcileProviderCredentials(ctx, bs.Namespace)
	if err != nil {
		return nil, errorUtil.Wrapf(err, "failed to reconcile aws blob storage provider credentials for blob storage instance %s", bs.Name)
	}

	// setup aws s3 sdk session
	p.Logger.Infof("creating new aws sdk session in region %s", stratCfg.Region)
	sess := session.Must(session.NewSession(&aws.Config{
		Region:      aws.String(stratCfg.Region),
		Credentials: credentials.NewStaticCredentials(providerCreds.AccessKeyID, providerCreds.SecretAccessKey, ""),
	}))
	s3svc := s3.New(sess)

	// the aws access key can sometimes still not be registered in aws on first try, so loop
	p.Logger.Infof("listing existing aws s3 buckets")
	var existingBuckets []*s3.Bucket
	err = wait.PollImmediate(time.Second*5, time.Minute*5, func() (done bool, err error) {
		listOutput, err := s3svc.ListBuckets(nil)
		if err != nil {
			return false, nil
		}
		existingBuckets = listOutput.Buckets
		return true, nil
	})
	if err != nil {
		return nil, errorUtil.Wrapf(err, "timed out waiting to list s3 buckets, searching for blob storage instance %s", bs.Name)
	}

	// pre-create the blobstorageinstance that will be returned if everything is successful
	bsi := &providers.BlobStorageInstance{
		DeploymentDetails: &BlobStorageDeploymentDetails{
			BucketName:          *bucketCreateCfg.Bucket,
			CredentialKeyID:     endUserCreds.AccessKeyID,
			CredentialSecretKey: endUserCreds.SecretAccessKey,
		},
	}

	// create bucket if it doesn't already exist, if it does exist then use the existing bucket
	p.Logger.Infof("checking if aws s3 bucket %s already exists", *bucketCreateCfg.Bucket)
	var foundBucket *s3.Bucket
	for _, b := range existingBuckets {
		if *b.Name == *bucketCreateCfg.Bucket {
			foundBucket = b
			break
		}
	}
	if foundBucket != nil {
		p.Logger.Infof("bucket %s already exists, using that", *foundBucket.Name)
		return bsi, nil
	}
	p.Logger.Infof("bucket %s not found, creating bucket", *bucketCreateCfg.Bucket)
	_, err = s3svc.CreateBucket(bucketCreateCfg)
	if err != nil {
		return nil, errorUtil.Wrapf(err, "failed to create s3 bucket %s, for blob storage instance %s", *bucketCreateCfg.Bucket, bs.Name)
	}
	p.Logger.Infof("creation handler for blob storage instance %s in namespace %s finished successfully", bs.Name, bs.Namespace)
	return bsi, nil
}

// DeleteStorage Delete S3 bucket and credentials to add objects to it
func (p *BlobStorageProvider) DeleteStorage(ctx context.Context, bs *v1alpha1.BlobStorage) error {
	p.Logger.Infof("deleting blob storage instance %s via aws s3", bs.Name)
	// resolve bucket information for bucket created by provider
	p.Logger.Infof("getting aws s3 bucket config for blob storage instance %s", bs.Name)
	bucketCreateCfg, stratCfg, err := p.getS3BucketConfig(ctx, bs)
	if err != nil {
		return errorUtil.Wrapf(err, "failed to retrieve aws s3 bucket config for blob storage instance %s", bs.Name)
	}
	if bucketCreateCfg.Bucket == nil {
		bucketCreateCfg.Bucket = aws.String(fmt.Sprintf("%s-%s", bs.Namespace, bs.Name))
	}

	// get provider aws creds so the bucket can be deleted
	p.Logger.Infof("creating provider credentials for creating s3 buckets, in namespace %s", bs.Namespace)
	providerCreds, err := p.CredentialManager.ReconcileProviderCredentials(ctx, bs.Namespace)
	if err != nil {
		return errorUtil.Wrapf(err, "failed to reconcile aws provider credentials for blob storage instance %s", bs.Name)
	}
	sess := session.Must(session.NewSession(&aws.Config{
		Region:      aws.String(stratCfg.Region),
		Credentials: credentials.NewStaticCredentials(providerCreds.AccessKeyID, providerCreds.SecretAccessKey, ""),
	}))

	// delete the bucket that was created by the provider
	p.Logger.Infof("creating new aws sdk session in region %s", stratCfg.Region)
	s3svc := s3.New(sess)

	_, err = s3svc.DeleteBucket(&s3.DeleteBucketInput{
		Bucket: bucketCreateCfg.Bucket,
	})
	s3err, isAWSErr := err.(awserr.Error)
	if err != nil && !isAWSErr {
		return errorUtil.Wrapf(err, "failed to delete s3 bucket %s", *bucketCreateCfg.Bucket)
	}
	if err != nil && isAWSErr {
		if s3err.Code() != s3.ErrCodeNoSuchBucket {
			return errorUtil.Wrapf(err, "failed to delete aws s3 bucket %s, aws error", *bucketCreateCfg.Bucket)
		}
	}
	err = s3svc.WaitUntilBucketNotExists(&s3.HeadBucketInput{
		Bucket: bucketCreateCfg.Bucket,
	})
	if err != nil {
		return errorUtil.Wrapf(err, "failed to wait for s3 bucket deletion, %s", *bucketCreateCfg.Bucket)
	}

	// remove the credentials request created by the provider
	endUserCredsName := fmt.Sprintf("cloud-resources-aws-s3-%s-credentials", bs.Name)
	p.Logger.Infof("deleting end-user credential request %s in namespace %s", endUserCredsName, bs.Namespace)
	endUserCredsReq := &v1.CredentialsRequest{
		ObjectMeta: controllerruntime.ObjectMeta{
			Name:      endUserCredsName,
			Namespace: bs.Namespace,
		},
	}
	if err := p.Client.Delete(ctx, endUserCredsReq); err != nil {
		return errorUtil.Wrapf(err, "failed to delete credential request %s", endUserCredsName)
	}

	// remove the finalizer added by the provider
	p.Logger.Infof("deleting finalizer %s from blob storage instance %s in namespace %s", DefaultFinalizer, bs.Name, bs.Namespace)
	resources.RemoveFinalizer(&bs.ObjectMeta, DefaultFinalizer)
	if err := p.Client.Update(ctx, bs); err != nil {
		return errorUtil.Wrapf(err, "failed to update instance %s as part of finalizer reconcile", bs.Name)
	}
	p.Logger.Infof("deletion handler for blob storage instance %s in namespace %s finished successfully", bs.Name, bs.Namespace)
	return nil
}

func (p *BlobStorageProvider) getS3BucketConfig(ctx context.Context, bs *v1alpha1.BlobStorage) (*s3.CreateBucketInput, *StrategyConfig, error) {
	stratCfg, err := p.ConfigManager.ReadBlobStorageStrategy(ctx, bs.Spec.Tier)
	if err != nil {
		return nil, nil, errorUtil.Wrap(err, "failed to read aws strategy config")
	}
	if stratCfg.Region == "" {
		p.Logger.Debugf("region not set in deployment strategy configuration, using default region %s", DefaultRegion)
		stratCfg.Region = DefaultRegion
	}

	// delete the s3 bucket created by the provider
	s3cbi := &s3.CreateBucketInput{}
	if err = json.Unmarshal(stratCfg.RawStrategy, s3cbi); err != nil {
		return nil, nil, errorUtil.Wrap(err, "failed to unmarshal aws s3 configuration")
	}
	return s3cbi, stratCfg, nil
}