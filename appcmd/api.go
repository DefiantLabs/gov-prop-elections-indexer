package appcmd

import (
	"fmt"
	"sync"
	"time"

	"github.com/DefiantLabs/cosmos-indexer/cmd"
	"github.com/DefiantLabs/cosmos-indexer/config"
	"github.com/DefiantLabs/gov-prop-elections-indexer/appconfig"
	"github.com/DefiantLabs/gov-prop-elections-indexer/database"
	"github.com/DefiantLabs/gov-prop-elections-indexer/requests"
	"github.com/gin-gonic/gin"
	"github.com/shopspring/decimal"
	"github.com/spf13/cobra"
	"gorm.io/gorm"
)

var apiConfig appconfig.APIConfig
var apiDB *gorm.DB

func init() {
	apiConfig = appconfig.APIConfig{}

	config.SetupDatabaseFlags(&apiConfig.Database, APICmd)
	config.SetupProbeFlags(&apiConfig.Probe, APICmd)
	appconfig.SetupLCDFlags(&apiConfig.LCD, APICmd)
	appconfig.SetupHostFlags(&apiConfig.Host, APICmd)
	appconfig.SetupProposalFlags(&apiConfig.ProposalsConfig, APICmd)
}

var APICmd = &cobra.Command{
	Use:     "api",
	Short:   "Runs the API to interface with the indexed dataset.",
	Long:    `Runs an API at a specific port on the system. The API provides helpful interfaces to query the indexed dataset and retrieve data in a structured format`,
	PreRunE: setupAPI,
	Run:     api,
}

func setupAPI(command *cobra.Command, args []string) error {
	config.Log.Infof("Binding config flags")
	cmd.BindFlags(command, cmd.GetViperConfig())
	config.Log.Infof("Bound config flags")

	var err error
	indexer := cmd.GetBuiltinIndexer()
	config.Log.Infof("Connecting to database and running migrationsc")
	apiDB, err = database.ConnectToDBAndRunMigrations(&apiConfig.Database, indexer.CustomModels)
	if err != nil {
		config.Log.Fatal("Failed to connect to database and run migrations", err)
	}
	config.Log.Infof("Connected to database and ran mgirations")

	config.DoConfigureLogger("./api.log", apiConfig.Host.LogLevel, true)
	config.Log.Infof("Logging configured")

	return nil
}

func CORSMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Probably want to lock CORs down later, will need to know the hostname of the UI server
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Allow-Credentials", "true")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, accept, origin, Cache-Control, X-Requested-With")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS, GET, PUT")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}

		c.Next()
	}
}

func api(cmd *cobra.Command, args []string) {

	config.SetChainConfig(apiConfig.Probe.AccountPrefix)

	r := gin.New()
	r.Use(gin.Recovery())
	r.Use(CORSMiddleware())
	r.GET("/gcphealth", Healthcheck)

	config.Log.Infof("Initializing API group")
	//Create api routing group
	api := r.Group("/api")

	api.GET("/proposals", proposals)
	api.POST("/proposals-metadata-by-ids", proposalsMetadataByIDs)
	api.POST("/proposals-by-ids", proposalsByIDs)
	api.POST("/proposal-statuses-by-ids", proposalStatuses)
	api.POST("/votes-by-proposal-ids", getVotesByProposalIDs)
	api.GET("/proposal-ids-by-candidate", proposalsByCandidate)

	config.Log.Infof("Initialized API group, starting server")
	err := r.Run(fmt.Sprintf("%s:%d", apiConfig.Host.Host, apiConfig.Host.Port))

	if err != nil {
		config.Log.Fatal("Error starting server.", err)
	}
}

func Healthcheck(context *gin.Context) {
	context.JSON(200, gin.H{"status": "ok"})
}

func proposalsByCandidate(c *gin.Context) {
	config.Log.Infof("Received request for proposals by candidate")

	mapping := make(map[string]*uint)

	if apiConfig.ProposalsConfig.ClydeProposal != 0 {
		mapping["clyde"] = &apiConfig.ProposalsConfig.ClydeProposal
	} else {
		mapping["clyde"] = nil
	}

	if apiConfig.ProposalsConfig.GraceProposal != 0 {
		mapping["grace"] = &apiConfig.ProposalsConfig.GraceProposal
	} else {
		mapping["grace"] = nil
	}

	if apiConfig.ProposalsConfig.MattProposal != 0 {
		mapping["matt"] = &apiConfig.ProposalsConfig.MattProposal
	} else {
		mapping["matt"] = nil
	}

	c.JSON(200, mapping)
}

var onChainProposalsCache = make(map[uint64]*requests.OnChainProposal)

func proposals(c *gin.Context) {
	config.Log.Infof("Received request for proposals")

	// Get proposals from the database
	var proposals []database.Proposal

	err := apiDB.Find(&proposals).Error

	if err != nil {
		c.JSON(500, gin.H{"error": "Failed to retrieve proposals"})
		return
	}

	var onChainProposals []requests.OnChainProposal

	for _, proposal := range proposals {
		if _, ok := onChainProposalsCache[proposal.ID]; !ok {
			onChainProposal, err := requests.GetOnChainProposalByID(proposal.ProposalID, apiConfig.LCD.URL)

			if err != nil {
				c.JSON(500, gin.H{"error": "Failed to retrieve proposals"})
				return
			} else {
				cacheOnChainProposal(proposal.ID, onChainProposal)
			}
		}

		onChainProposals = append(onChainProposals, *onChainProposalsCache[proposal.ID])
	}

	c.JSON(200, onChainProposals)
}

type proposalsByIDsRequest struct {
	ProposalIDs []int `json:"proposal_ids"`
}

type proposalsByIDsResponse struct {
	Proposal  requests.OnChainProposal                        `json:"proposal"`
	VoteTally []database.GroupVotesByProposalAndBlockResponse `json:"vote_tally"`
}

type proposalsByIDsReturn struct {
	Proposals map[uint64]proposalsByIDsResponse `json:"proposals"`
}

func proposalsByIDs(c *gin.Context) {
	config.Log.Infof("Received request for proposals by IDs")
	// Load json request
	var request proposalsByIDsRequest
	err := c.BindJSON(&request)

	if err != nil {
		c.JSON(400, gin.H{"error": "Invalid request, must be a json object with a field 'proposal_ids' that is an array of integers"})
		return
	}

	// Get proposals from the database
	proposals, err := database.FindProposalsByIDs(apiDB, request.ProposalIDs)

	if err != nil {
		c.JSON(500, gin.H{"error": "Failed to retrieve proposals"})
		return
	}

	var proposalsByIDs = make(map[uint64]proposalsByIDsResponse)

	for _, proposal := range proposals {
		if _, ok := onChainProposalsCache[proposal.ID]; !ok {
			onChainProposal, err := requests.GetOnChainProposalByID(proposal.ProposalID, apiConfig.LCD.URL)

			if err != nil {
				c.JSON(500, gin.H{"error": "Failed to retrieve proposals"})
				return
			} else {
				cacheOnChainProposal(proposal.ID, onChainProposal)
			}
		}

		proposalsByIDs[proposal.ProposalID] = proposalsByIDsResponse{
			Proposal: *onChainProposalsCache[proposal.ID],
		}
	}

	var mappedTallies = make(map[uint64][]database.GroupVotesByProposalAndBlockResponse)

	tallies, err := database.GroupVotesByProposalAndBlock(apiDB, &proposals)

	if err != nil {
		c.JSON(500, gin.H{"error": "Failed to retrieve proposals"})
		return
	}

	// Bucketize the groups by proposal id, taking into account that some will be

	for _, proposal := range proposals {
		if _, ok := mappedTallies[proposal.ProposalID]; !ok {
			mappedTallies[proposal.ProposalID] = []database.GroupVotesByProposalAndBlockResponse{}
		}
	}

	if len(tallies) == 0 {
		var proposalsByIDsReturn = proposalsByIDsReturn{
			Proposals: proposalsByIDs,
		}
		c.JSON(200, proposalsByIDsReturn)
		return
	}

	var heightTracker int64 = tallies[0].Height
	heightSeenTracker := 0

	for _, tally := range tallies {
		currentHeight := tally.Height

		if currentHeight != heightTracker {
			if heightSeenTracker != len(proposals) {
				// We need to fill in the gaps where there are no tallies for the previous height
				for _, proposal := range proposals {
					if len(mappedTallies[proposal.ProposalID]) == 0 {
						mappedTallies[proposal.ProposalID] = append(mappedTallies[proposal.ProposalID], database.GroupVotesByProposalAndBlockResponse{
							ProposalID:   nil,
							Height:       heightTracker,
							EmptyCount:   0,
							YesCount:     0,
							AbstainCount: 0,
							NoCount:      0,
							VetoCount:    0,
						})
					} else if mappedTallies[proposal.ProposalID][len(mappedTallies[proposal.ProposalID])-1].Height != heightTracker {
						mappedTallies[proposal.ProposalID] = append(mappedTallies[proposal.ProposalID], database.GroupVotesByProposalAndBlockResponse{
							ProposalID:   nil,
							Height:       heightTracker,
							EmptyCount:   0,
							YesCount:     0,
							AbstainCount: 0,
							NoCount:      0,
							VetoCount:    0,
						})
					}
				}
			}

			heightTracker = currentHeight
			heightSeenTracker = 0
		}

		if tally.ProposalID == nil {
			for _, proposal := range proposals {
				mappedTallies[proposal.ProposalID] = append(mappedTallies[proposal.ProposalID], tally)

				heightSeenTracker++
			}
		} else {
			mappedTallies[*tally.ProposalID] = append(mappedTallies[*tally.ProposalID], tally)
			heightSeenTracker++
		}
	}

	for _, proposal := range proposals {
		voteTally := mappedTallies[proposal.ProposalID]
		currentProposal := proposalsByIDs[proposal.ProposalID]
		currentProposal.VoteTally = voteTally
		proposalsByIDs[proposal.ProposalID] = currentProposal
	}

	var proposalsByIDsReturn = proposalsByIDsReturn{
		Proposals: proposalsByIDs,
	}

	c.JSON(200, proposalsByIDsReturn)
}

var cacheMutex = &sync.Mutex{}

func cacheOnChainProposal(proposalID uint64, proposal *requests.OnChainProposal) {
	cacheMutex.Lock()
	defer cacheMutex.Unlock()

	onChainProposalsCache[proposalID] = proposal
}

type proposalsMetadataByIDsResponse struct {
	VoteTotals       database.DistinctVoteOptionCounts           `json:"vote_totals"`
	AddressTotals    []database.CountDistinctAddressesScanResult `json:"address_totals"`
	Proposals        map[uint64]requests.OnChainProposal         `json:"proposals"`
	ProposalStatuses proposalStatusesTotals                      `json:"proposal_statuses"`
	UniqueAddresses  int                                         `json:"unique_addresses"`
}

type proposalStatusesTotals struct {
	YesAmount     decimal.Decimal                                              `json:"yes_amount"`
	NoAmount      decimal.Decimal                                              `json:"no_amount"`
	AbstainAmount decimal.Decimal                                              `json:"abstain_amount"`
	EmptyAmount   decimal.Decimal                                              `json:"empty_amount"`
	VetoAmount    decimal.Decimal                                              `json:"veto_amount"`
	TotalAmount   decimal.Decimal                                              `json:"total_amount"`
	Proposals     map[uint]database.LatestProposalStatusByProposalIDScanResult `json:"proposals"`
}

func proposalsMetadataByIDs(c *gin.Context) {
	config.Log.Infof("Received request for proposals metadata by IDs")
	// Load json request
	var request proposalsByIDsRequest
	err := c.BindJSON(&request)

	if err != nil {
		c.JSON(400, gin.H{"error": "Invalid request, must be a json object with a field 'proposal_ids' that is an array of integers"})
		return
	}

	// Get proposals from the database
	proposals, err := database.FindProposalsByIDs(apiDB, request.ProposalIDs)

	if err != nil {
		c.JSON(500, gin.H{"error": "Failed to retrieve proposals"})
		return
	}

	var proposalsByIDs = make(map[uint64]requests.OnChainProposal)

	for _, proposal := range proposals {
		if _, ok := onChainProposalsCache[proposal.ID]; !ok {
			onChainProposal, err := requests.GetOnChainProposalByID(proposal.ProposalID, apiConfig.LCD.URL)

			if err != nil {
				c.JSON(500, gin.H{"error": "Failed to retrieve proposals"})
				return
			} else {
				cacheOnChainProposal(proposal.ID, onChainProposal)
			}
		}

		proposalsByIDs[proposal.ProposalID] = *onChainProposalsCache[proposal.ID]
	}

	voteTotals, err := database.CountDistinctVoteOptionsByProposals(apiDB, &proposals)

	if err != nil {
		c.JSON(500, gin.H{"error": "Failed to retrieve proposals"})
		return
	}

	mappedVoteTotals := make(map[uint64]database.CountDistinctVotesScanResult)

	distinctVoteOptionCounts := database.DistinctVoteOptionCounts{
		Proposals: mappedVoteTotals,
	}

	for _, proposal := range voteTotals {
		proposalID := proposal.ProposalID
		mappedVoteTotals[proposalID] = proposal

		distinctVoteOptionCounts.AbstainCount += int64(proposal.AbstainCount)
		distinctVoteOptionCounts.EmptyCount += int64(proposal.EmptyCount)
		distinctVoteOptionCounts.NoCount += int64(proposal.NoCount)
		distinctVoteOptionCounts.VetoCount += int64(proposal.VetoCount)
		distinctVoteOptionCounts.YesCount += int64(proposal.YesCount)

	}

	addressTotals, err := database.CountDistinctAddressesByProposal(apiDB, &proposals)

	if err != nil {
		c.JSON(500, gin.H{"error": "Failed to retrieve proposals"})
		return
	}

	uniqueAddresses, err := database.CountDistinctAddresses(apiDB)

	if err != nil {
		c.JSON(500, gin.H{"error": "Failed to retrieve proposals"})
		return
	}

	proposalStatusTotals, err := database.GetLatestProposalStatusesByProposalID(apiDB, request.ProposalIDs)

	if err != nil {
		c.JSON(500, gin.H{"error": "Failed to retrieve proposals"})
		return
	}

	proposalStatusesTotals := proposalStatusesTotals{}
	proposalStatusMap := make(map[uint]database.LatestProposalStatusByProposalIDScanResult)

	for _, status := range proposalStatusTotals {
		proposalStatusMap[status.ProposalID] = status

		proposalStatusesTotals.AbstainAmount = proposalStatusesTotals.AbstainAmount.Add(status.AbstainAmount)
		proposalStatusesTotals.EmptyAmount = proposalStatusesTotals.EmptyAmount.Add(status.EmptyAmount)
		proposalStatusesTotals.NoAmount = proposalStatusesTotals.NoAmount.Add(status.NoAmount)
		proposalStatusesTotals.VetoAmount = proposalStatusesTotals.VetoAmount.Add(status.VetoAmount)
		proposalStatusesTotals.YesAmount = proposalStatusesTotals.YesAmount.Add(status.YesAmount)
		proposalStatusesTotals.TotalAmount = proposalStatusesTotals.TotalAmount.Add(status.AbstainAmount)
		proposalStatusesTotals.TotalAmount = proposalStatusesTotals.TotalAmount.Add(status.EmptyAmount)
		proposalStatusesTotals.TotalAmount = proposalStatusesTotals.TotalAmount.Add(status.NoAmount)
		proposalStatusesTotals.TotalAmount = proposalStatusesTotals.TotalAmount.Add(status.VetoAmount)
		proposalStatusesTotals.TotalAmount = proposalStatusesTotals.TotalAmount.Add(status.YesAmount)
	}

	proposalStatusesTotals.Proposals = proposalStatusMap

	var proposalsMetadataByIDsReturn = proposalsMetadataByIDsResponse{
		VoteTotals:       distinctVoteOptionCounts,
		Proposals:        proposalsByIDs,
		AddressTotals:    []database.CountDistinctAddressesScanResult{},
		ProposalStatuses: proposalStatusesTotals,
		UniqueAddresses:  uniqueAddresses,
	}

	if len(addressTotals) > 0 {
		proposalsMetadataByIDsReturn.AddressTotals = addressTotals
	}

	c.JSON(200, proposalsMetadataByIDsReturn)
}

type votesByProposalIDsRequest struct {
	ProposalIDs []int `json:"proposal_ids"`
	Page        int   `json:"page"`
	PerPage     int   `json:"per_page"`
}

func getVotesByProposalIDs(c *gin.Context) {
	config.Log.Infof("Received request for votes by proposals IDs")

	var request votesByProposalIDsRequest
	err := c.BindJSON(&request)

	if err != nil {
		c.JSON(400, gin.H{"error": "Invalid request, must be a json object with a field 'proposal_ids' that is an array of integers"})
		return
	}

	var page = request.Page
	var perPage = request.PerPage

	if request.Page < 0 || request.PerPage < 0 {
		c.JSON(400, gin.H{"error": "Invalid request, page and per_page must be greater than or equal to 0"})
		return
	}

	if request.PerPage > 100 {
		c.JSON(400, gin.H{"error": "Invalid request, per_page must be less than or equal to 100"})
		return
	}

	if request.Page == 0 && request.PerPage == 0 {
		page = 1
		perPage = 10
	}

	// Translate page and per page to limit and offset with 1-indexed pages assumption
	currentOffset := (page - 1) * perPage
	currentLimit := perPage

	// Get proposals from the database
	proposals, err := database.FindProposalsByIDs(apiDB, request.ProposalIDs)

	if err != nil {
		c.JSON(500, gin.H{"error": "Failed to retrieve proposals"})
		return
	}

	votes, err := database.GetVotesPaginated(apiDB, currentOffset, currentLimit, proposals)

	if err != nil {
		c.JSON(500, gin.H{"error": "Failed to retrieve votes"})
		return
	}

	if len(votes) == 0 {
		votes = make([]database.GetVotesPaginatedScanResult, 0)
	}
	c.JSON(200, votes)
}

type statusesByProposalIDsRequest struct {
	ProposalIDs []uint `json:"proposal_ids"`
}

type statusesByProposalIDsResponse struct {
	Proposals  map[uint][]database.ProposalStatusByProposalID `json:"proposals"`
	Heights    []uint                                         `json:"heights"`
	Timestamps []time.Time                                    `json:"timestamps"`
}

func proposalStatuses(c *gin.Context) {
	config.Log.Infof("Received request for votes by proposals statuses")

	var request statusesByProposalIDsRequest
	err := c.BindJSON(&request)

	if err != nil {
		c.JSON(400, gin.H{"error": "Invalid request, must be a json object with a field 'proposal_ids' that is an array of integers"})
		return
	}

	statuses, err := database.GetProposalStatusesByProposalID(apiDB, request.ProposalIDs)

	if err != nil {
		c.JSON(500, gin.H{"error": "Failed to retrieve proposals"})
		return
	}

	mappedStatuses := make(map[uint][]database.ProposalStatusByProposalID)

	for _, proposal := range request.ProposalIDs {
		mappedStatuses[proposal] = []database.ProposalStatusByProposalID{}
	}

	if len(statuses) == 0 {
		heights := make([]uint, 0)
		timestamps := make([]time.Time, 0)
		c.JSON(200, statusesByProposalIDsResponse{
			Proposals:  mappedStatuses,
			Heights:    heights,
			Timestamps: timestamps,
		})
		return
	}

	currentHeight := statuses[0].Height
	currentTimeStamp := statuses[0].TimeStamp
	trackingMap := make(map[uint]bool)
	allHeights := []uint{currentHeight}
	allTimestamps := []time.Time{currentTimeStamp}
	for i, status := range statuses {
		if status.Height != currentHeight {
			if len(trackingMap) != len(request.ProposalIDs) {
				for _, proposal := range request.ProposalIDs {
					if _, ok := trackingMap[proposal]; !ok {

						if len(mappedStatuses[proposal]) == 0 {
							mappedStatuses[proposal] = append(mappedStatuses[proposal], database.ProposalStatusByProposalID{
								ProposalID: proposal,
								Height:     currentHeight,
								TimeStamp:  currentTimeStamp,
							})
						} else {
							mappedStatuses[proposal] = append(mappedStatuses[proposal], database.ProposalStatusByProposalID{
								ProposalID:    proposal,
								Height:        currentHeight,
								TimeStamp:     currentTimeStamp,
								YesAmount:     mappedStatuses[proposal][len(mappedStatuses[proposal])-1].YesAmount,
								NoAmount:      mappedStatuses[proposal][len(mappedStatuses[proposal])-1].NoAmount,
								EmptyAmount:   mappedStatuses[proposal][len(mappedStatuses[proposal])-1].EmptyAmount,
								AbstainAmount: mappedStatuses[proposal][len(mappedStatuses[proposal])-1].AbstainAmount,
								VetoAmount:    mappedStatuses[proposal][len(mappedStatuses[proposal])-1].VetoAmount,
							})
						}
					}
				}
			}

			currentHeight = status.Height
			currentTimeStamp = status.TimeStamp
			trackingMap = make(map[uint]bool)

			allHeights = append(allHeights, currentHeight)
			allTimestamps = append(allTimestamps, currentTimeStamp)
		}

		mappedStatuses[status.ProposalID] = append(mappedStatuses[status.ProposalID], status)

		trackingMap[status.ProposalID] = true

		if i == len(statuses)-1 {
			for _, proposal := range request.ProposalIDs {
				if _, ok := trackingMap[proposal]; !ok {
					if len(mappedStatuses[proposal]) == 0 {
						mappedStatuses[proposal] = append(mappedStatuses[proposal], database.ProposalStatusByProposalID{
							ProposalID: proposal,
							Height:     currentHeight,
							TimeStamp:  currentTimeStamp,
						})
					} else {
						mappedStatuses[proposal] = append(mappedStatuses[proposal], database.ProposalStatusByProposalID{
							ProposalID:    proposal,
							Height:        currentHeight,
							TimeStamp:     currentTimeStamp,
							YesAmount:     mappedStatuses[proposal][len(mappedStatuses[proposal])-1].YesAmount,
							NoAmount:      mappedStatuses[proposal][len(mappedStatuses[proposal])-1].NoAmount,
							EmptyAmount:   mappedStatuses[proposal][len(mappedStatuses[proposal])-1].EmptyAmount,
							AbstainAmount: mappedStatuses[proposal][len(mappedStatuses[proposal])-1].AbstainAmount,
							VetoAmount:    mappedStatuses[proposal][len(mappedStatuses[proposal])-1].VetoAmount,
						})
					}
				}
			}

		}
	}

	ret := statusesByProposalIDsResponse{
		Proposals:  mappedStatuses,
		Heights:    allHeights,
		Timestamps: allTimestamps,
	}
	c.JSON(200, ret)
}
