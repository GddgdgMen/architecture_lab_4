package integration

import (
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

const baseAddress = "http://balancer:8090"

var client = http.Client{
	Timeout: 3 * time.Second,
}

type IntegrationTestSuite struct {
	suite.Suite
}

func TestBalancer(t *testing.T) {
	suite.Run(t, new(IntegrationTestSuite))
}

func (s *IntegrationTestSuite) TestGetRequest() {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		s.T().Skip("Integration test is not enabled")
	}

	servers := []string{"server1:8080", "server2:8080", "server3:8080"}
	serverCount := len(servers)

	for i := 0; i < 10; i++ {
		resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data", baseAddress))
		assert.NoError(s.T(), err)

		expectedServer := servers[i%serverCount]
		//assert.Equal(s.T(), expectedServer, resp.Header.Get("lb-from"))
	}
}

func (s *IntegrationTestSuite) BenchmarkBalancer(b *testing.B) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		s.T().Skip("Integration test is not enabled")
	}

	b.ResetTimer()

	req, err := http.NewRequest("GET", fmt.Sprintf("%s/api/v1/some-data", baseAddress), nil)
	assert.NoError(s.T(), err)

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			resp, err := client.Do(req)
			assert.NoError(s.T(), err)
			resp.Body.Close()
		}
	})

	b.StopTimer()
}
