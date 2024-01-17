package create

type QueryCreateDatabase struct {
	*QueryCreate
	Name string `json:"name"`
}
