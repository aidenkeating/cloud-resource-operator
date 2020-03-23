package azure

import (
	"context"
	"github.com/integr8ly/cloud-resource-operator/pkg/apis/integreatly/v1alpha1"
	"github.com/integr8ly/cloud-resource-operator/pkg/apis/integreatly/v1alpha1/types"
	"github.com/integr8ly/cloud-resource-operator/pkg/providers"
	"github.com/integr8ly/cloud-resource-operator/pkg/resources"
	"github.com/sirupsen/logrus"
	v1 "k8s.io/api/core/v1"
	types2 "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"time"
)

var _ providers.PostgresProvider = &PostgresProvider{}

type PostgresProvider struct {
	Logger *logrus.Entry
	OpenShiftClient client.Client
}

func NewDefaultPostgresProvider(logger *logrus.Entry, client client.Client) *PostgresProvider {
	return &PostgresProvider{
		Logger:logger,
		OpenShiftClient:client,
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
	p.Logger.Debug("creating postgres")

	credsSecret := &v1.Secret{}
	if err := p.OpenShiftClient.Get(ctx, types2.NamespacedName{Name: "azure-creds", Namespace: ps.Namespace}, credsSecret); err != nil {
		return nil, "error", err
	}
	p.Logger.Debugf("azure creds, key=%s, secret=%s", credsSecret.Data["key"], credsSecret.Data["secret"])

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
