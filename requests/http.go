package requests

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"time"

	tmjson "github.com/cometbft/cometbft/libs/json"
)

func GetOnChainProposalByID(id uint64, lcdUrl string) (*OnChainProposal, error) {
	var client = URIClient{
		Address: lcdUrl,
		Client:  &http.Client{},
	}

	if client.Address == "" {
		return nil, fmt.Errorf("no LCD URL provided")
	}

	var endpoint = fmt.Sprintf("cosmos/gov/v1/proposals/%d", id)

	resp, err := client.DoHTTPGet(context.TODO(), endpoint, nil)

	if err != nil {
		return nil, err
	}

	var proposal OnChainProposalResp

	err = json.Unmarshal(resp, &proposal)

	if err != nil {
		return nil, err
	}

	if proposal.Code != nil && *proposal.Code != 0 {
		return nil, fmt.Errorf("failed to get proposal: %v", *proposal.Code)
	}

	return proposal.Proposal, err
}

type URIClient struct {
	Address    string
	Client     *http.Client
	AuthHeader string
}

func argsToURLValues(args map[string]interface{}) (url.Values, error) {
	values := make(url.Values)
	if len(args) == 0 {
		return values, nil
	}

	err := argsToJSON(args)
	if err != nil {
		return nil, err
	}

	for key, val := range args {
		values.Set(key, val.(string))
	}

	return values, nil
}

func argsToJSON(args map[string]interface{}) error {
	for k, v := range args {
		rt := reflect.TypeOf(v)
		isByteSlice := rt.Kind() == reflect.Slice && rt.Elem().Kind() == reflect.Uint8
		if isByteSlice {
			bytes := reflect.ValueOf(v).Bytes()
			args[k] = fmt.Sprintf("0x%X", bytes)
			continue
		}

		data, err := tmjson.Marshal(v)
		if err != nil {
			return err
		}
		args[k] = string(data)
	}
	return nil
}

func (c *URIClient) DoHTTPGet(ctx context.Context, method string, params map[string]interface{}) ([]byte, error) {
	values, err := argsToURLValues(params)
	if err != nil {
		return nil, fmt.Errorf("failed to encode params: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.Address+"/"+method, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating new request: %w", err)
	}

	req.URL.RawQuery = values.Encode()
	// fmt.Printf("Query string: %s\n", values.Encode())

	// req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if c.AuthHeader != "" {
		req.Header.Add("Authorization", c.AuthHeader)
	}

	resp, err := c.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("get: %w", err)
	}
	defer resp.Body.Close()

	responseBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read response body: %w", err)
	}

	return responseBytes, nil
}

type OnChainProposalResp struct {
	Code     *uint            `json:"code,omitempty"`
	Proposal *OnChainProposal `json:"proposal,omitempty"`
}

type OnChainProposal struct {
	ID               string            `json:"id"`
	SubmitTime       time.Time         `json:"submit_time"`
	VotingStartTime  time.Time         `json:"voting_start_time"`
	VotingEndTime    time.Time         `json:"voting_end_time"`
	Title            string            `json:"title"`
	Summary          string            `json:"summary"`
	FinalTallyResult OnChainFinalTally `json:"final_tally_result"`
}

type OnChainFinalTally struct {
	Yes        string `json:"yes_count"`
	Abstain    string `json:"abstain_count"`
	No         string `json:"no_count"`
	NoWithVeto string `json:"no_with_veto_count"`
}
