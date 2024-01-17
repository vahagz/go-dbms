package drop

type QueryDropDatabase struct {
	*QueryDrop
	DB string `json:"db"`
}
