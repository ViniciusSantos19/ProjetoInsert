package model

import (
	"time"
)

type Checkin struct {
	UserID  string    `db:"UserID"`
	TweetID string    `db:"TweetID"`
	Lat     float64   `db:"Lat"`
	Long    float64   `db:"Long"`
	Time    time.Time `db:"Time"`
	VenueID string    `db:"VenueID"`
	Text    string    `db:"Text"`
}
