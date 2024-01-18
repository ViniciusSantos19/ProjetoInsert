package model

import (
	"time"
)

type Checkin struct {
	UserID  string    `json:"userID"`
	TweetID string    `json:"tweetID"`
	Lat     float64   `json:"lat"`
	Long    float64   `json:"long"`
	Time    time.Time `json:"time"`
	VenueID string    `json:"venueID"`
	Text    string    `json:"text"`
}
