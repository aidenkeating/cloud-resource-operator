package azure

import (
	"context"
	"os"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"time"

	"github.com/Azure/azure-sdk-for-go/profiles/latest/postgresql/mgmt/postgresql"
	"github.com/Azure/go-autorest/autorest/azure/auth"
	"github.com/integr8ly/cloud-resource-operator/pkg/apis/integreatly/v1alpha1"
	"github.com/integr8ly/cloud-resource-operator/pkg/apis/integreatly/v1alpha1/types"
	"github.com/integr8ly/cloud-resource-operator/pkg/providers"
	"github.com/integr8ly/cloud-resource-operator/pkg/resources"

	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	types2 "k8s.io/apimachinery/pkg/types"
)

var _ providers.PostgresProvider = &PostgresProvider{}

type PostgresProvider struct {
	Logger          *logrus.Entry
	OpenShiftClient client.Client
}

func NewDefaultPostgresProvider(client client.Client, logger *logrus.Entry) *PostgresProvider {
	return &PostgresProvider{
		Logger:          logger,
		OpenShiftClient: client,
	}
}

func (p PostgresProvider) GetName() string {
	return "Azure Postgres Provider"
}

func (p PostgresProvider) SupportsStrategy(s string) bool {
	return s == "azure"
}

func (p PostgresProvider) GetReconcileTime(ps *v1alpha1.Postgres) time.Duration {
	return resources.GetForcedReconcileTimeOrDefault(time.Second * 30)
}

func (p PostgresProvider) CreatePostgres(ctx context.Context, ps *v1alpha1.Postgres) (*providers.PostgresInstance, types.StatusMessage, error) {
	p.Logger.Debug("creating azure postgres")

	azureCredSecret := &v1.Secret{}
	if err := p.OpenShiftClient.Get(ctx, types2.NamespacedName{Name: "azure-creds", Namespace: ps.Namespace}, azureCredSecret); err != nil {
		return nil, "error", err
	}

	// Set local environment variables for authentication
	os.Setenv("AZURE_SUBSCRIPTION_ID", string(azureCredSecret.Data["subscription_id"]))
	os.Setenv("AZURE_TENANT_ID", string(azureCredSecret.Data["tenant_id"]))
	os.Setenv("AZURE_CLIENT_ID", string(azureCredSecret.Data["client_id"]))
	os.Setenv("AZURE_CLIENT_SECRET", string(azureCredSecret.Data["client_secret_id"]))

	// Authenticate client based on set environment variables
	postgresClient := postgresql.NewServersClient("AZURE_SUBSCRIPTION_ID")

	authorizer, err := auth.NewAuthorizerFromEnvironment()
	if err == nil {
		postgresClient.Authorizer = authorizer
	}
	// ctx, _ := context.WithTimeout(context.Background(), 300*time.Second)

	// p.Logger.Debugf("azure creds, SubID=%s, TenantID=%s, ClientID=%s, ClientSecretID=%s", os.Getenv("AZURE_SUBSCRIPTION_ID"), os.Getenv("AZURE_TENANT_ID"), os.Getenv("AZURE_CLIENT_ID"), os.Getenv("AZURE_CLIENT_SECRET"))

	return &providers.PostgresInstance{
		DeploymentDetails: &providers.PostgresDeploymentDetails{
			Username: "test",
			Password: "test",
			Host:     "test",
			Database: "test",
			Port:     123,
		},
	}, "completed", nil
}

func (p PostgresProvider) DeletePostgres(ctx context.Context, ps *v1alpha1.Postgres) (types.StatusMessage, error) {
	panic("implement me")
}
