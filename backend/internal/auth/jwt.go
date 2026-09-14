package auth

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

var ErrDisabled = errors.New("jwt auth disabled")
var ErrInvalid = errors.New("invalid token")

type Claims struct {
	PlayerID string `json:"pid"`
	Name     string `json:"name"`
	jwt.RegisteredClaims
}

type JWT struct {
	secret []byte
	ttl    time.Duration
}

func NewJWT(secret string, ttl time.Duration) (*JWT, error) {
	if secret == "" {
		return nil, ErrDisabled
	}
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	return &JWT{secret: []byte(secret), ttl: ttl}, nil
}

// Issue mints a token for a brand-new anonymous session. The id is random and
// means nothing tomorrow — see IssueFor for the other case.
func (j *JWT) Issue(name string) (token, playerID string, expiresAt time.Time, err error) {
	playerID, err = newPlayerID()
	if err != nil {
		return "", "", time.Time{}, err
	}
	return j.IssueFor(playerID, name)
}

// IssueFor mints a token for an id that already exists — an account the
// platform tier authenticated.
//
// The split matters because Issue mints the id itself, and an identity a
// process invents is exactly what the rest of this repo spent some effort
// getting rid of: two gateways counting independently hand the same id to two
// different people. When there is an account behind the login, the id comes
// from the account and this is the only thing that puts it in the token.
func (j *JWT) IssueFor(playerID, name string) (token, id string, expiresAt time.Time, err error) {
	if playerID == "" {
		return "", "", time.Time{}, ErrInvalid
	}
	if name == "" {
		name = "player"
	}
	expiresAt = time.Now().Add(j.ttl)
	claims := Claims{
		PlayerID: playerID,
		Name:     name,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   playerID,
			ExpiresAt: jwt.NewNumericDate(expiresAt),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
		},
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := tok.SignedString(j.secret)
	if err != nil {
		return "", "", time.Time{}, err
	}
	return signed, playerID, expiresAt, nil
}

func (j *JWT) Verify(token string) (*Claims, error) {
	if token == "" {
		return nil, ErrInvalid
	}
	parsed, err := jwt.ParseWithClaims(token, &Claims{}, func(t *jwt.Token) (any, error) {
		if t.Method != jwt.SigningMethodHS256 {
			return nil, fmt.Errorf("unexpected signing method")
		}
		return j.secret, nil
	}, jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}))
	if err != nil || !parsed.Valid {
		return nil, ErrInvalid
	}
	claims, ok := parsed.Claims.(*Claims)
	if !ok || claims.PlayerID == "" {
		return nil, ErrInvalid
	}
	return claims, nil
}

func newPlayerID() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return "p-" + hex.EncodeToString(b[:]), nil
}
