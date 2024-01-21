package model

import (
	"time"
)

type Checkin struct {
	UserID  string    `db:"userID"`
	TweetID string    `db:"tweetID"`
	Lat     float64   `db:"lat"`
	Long    float64   `db:"long"`
	Time    time.Time `db:"time"`
	VenueID string    `db:"venueID"`
	Text    string    `db:"text"`
}
