package drop

type QueryDropIndex struct {
	*QueryDrop
	DB    string `json:"db"`
	Table string `json:"table"`
	Index string `josn:"index"`
}
