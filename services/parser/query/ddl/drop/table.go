package drop

type QueryDropTable struct {
	*QueryDrop
	DB    string `json:"db"`
	Table string `json:"table"`
}
