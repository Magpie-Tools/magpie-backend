package models

import "net/url"

type Judge struct {
	Url   url.URL
	Ip    string
	Regex string
}
