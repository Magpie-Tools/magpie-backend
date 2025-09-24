package dto

// Credentials This is necessary to prevent any Mass Assignment Vulnerability attack
type Credentials struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}
