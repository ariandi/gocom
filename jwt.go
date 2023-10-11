package gocom

import (
	"fmt"
	"time"

	"github.com/ariandi/gocom/secret"
	"github.com/golang-jwt/jwt/v4"
)

func NewJWT(data map[string]interface{}, ttl ...time.Duration) (string, error) {

	targetTTL := 24 * time.Hour

	if len(ttl) > 0 {
		targetTTL = ttl[0]
	}

	pem, err := secret.Get("app.jwt.privatekey")

	if err != nil {
		return "", fmt.Errorf("Get jwt private key error: %w", err)
	}

	ret := ""

	privateKey, err := jwt.ParseRSAPrivateKeyFromPEM([]byte(pem))

	if err != nil {
		return "", fmt.Errorf("jwt error parse private key : %w", err)
	}

	now := time.Now().UTC()

	claims := make(jwt.MapClaims)

	for key, val := range data {
		claims[key] = val
	}

	claims["exp"] = now.Add(targetTTL).Unix()
	claims["iat"] = now.Unix()
	claims["nbf"] = now.Unix()

	ret, err = jwt.NewWithClaims(jwt.SigningMethodRS256, claims).SignedString(privateKey)
	if err != nil {
		return "", fmt.Errorf("jwt error sign token : %w", err)
	}

	return ret, nil
}

func ValidateJWT(token string) (map[string]interface{}, error) {

	pem, err := secret.Get("app.jwt.publickey")

	if err != nil {
		return nil, fmt.Errorf("Get jwt public key error: %w", err)
	}

	key, err := jwt.ParseRSAPublicKeyFromPEM([]byte(pem))
	if err != nil {
		return nil, fmt.Errorf("validate: parse key: %w", err)
	}

	tok, err := jwt.Parse(token, func(jwtToken *jwt.Token) (interface{}, error) {
		if _, ok := jwtToken.Method.(*jwt.SigningMethodRSA); !ok {
			return nil, fmt.Errorf("unexpected method: %s", jwtToken.Header["alg"])
		}

		return key, nil
	})
	if err != nil {
		return nil, fmt.Errorf("validate: %w", err)
	}

	claims, ok := tok.Claims.(jwt.MapClaims)
	if !ok || !tok.Valid {
		return nil, fmt.Errorf("validate: invalid")
	}

	return claims, nil
}
