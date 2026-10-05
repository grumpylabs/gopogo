package protocol

import (
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"strings"
)

// SCRAM-SHA-256 server side (RFC 5802, RFC 7677) for Postgres password
// authentication, so the password is never sent over the connection. Channel
// binding is not offered: clients use the "n" or "y" GS2 flag.

const scramIterations = 4096

var errSCRAM = errors.New("SCRAM authentication failed")

// scramAuth runs the SASL exchange for password; it returns errSCRAM when the
// client's proof does not match.
func (c *pgConn) scramAuth(password string) error {
	// AuthenticationSASL with the one mechanism offered.
	c.writeMsg('R', append(be32(10), "SCRAM-SHA-256\x00\x00"...))
	c.w.Flush()

	// SASLInitialResponse: mechanism, then the client-first-message.
	typ, body, err := c.readMessage()
	if err != nil {
		return err
	}
	m := &pgMsg{b: body}
	mech := m.cstr()
	n := m.int32()
	clientFirst := string(m.bytes(int(n)))
	if typ != 'p' || m.err || mech != "SCRAM-SHA-256" {
		return errSCRAM
	}
	// client-first-message = gs2-header client-first-message-bare, where the
	// gs2-header is "n,," or "y,," (no channel binding) plus an optional
	// authzid we ignore.
	parts := strings.SplitN(clientFirst, ",", 3)
	if len(parts) != 3 || (parts[0] != "n" && parts[0] != "y") {
		return errSCRAM
	}
	gs2Header := parts[0] + "," + parts[1] + ","
	clientFirstBare := parts[2]
	clientNonce := scramAttr(clientFirstBare, 'r')
	if clientNonce == "" {
		return errSCRAM
	}

	var salt, serverNonce [18]byte
	rand.Read(salt[:])
	rand.Read(serverNonce[:])
	nonce := clientNonce + base64.StdEncoding.EncodeToString(serverNonce[:])
	serverFirst := "r=" + nonce + ",s=" + base64.StdEncoding.EncodeToString(salt[:]) +
		",i=4096"
	c.writeMsg('R', append(be32(11), serverFirst...)) // AuthenticationSASLContinue
	c.w.Flush()

	// SASLResponse: client-final-message = c=<gs2>,r=<nonce>[,...],p=<proof>.
	typ, body, err = c.readMessage()
	if err != nil {
		return err
	}
	clientFinal := string(body)
	i := strings.LastIndex(clientFinal, ",p=")
	if typ != 'p' || i < 0 {
		return errSCRAM
	}
	withoutProof := clientFinal[:i]
	proof, err := base64.StdEncoding.DecodeString(clientFinal[i+3:])
	if err != nil || len(proof) != sha256.Size {
		return errSCRAM
	}
	if scramAttr(withoutProof, 'c') != base64.StdEncoding.EncodeToString([]byte(gs2Header)) ||
		scramAttr(withoutProof, 'r') != nonce {
		return errSCRAM
	}

	salted, err := pbkdf2.Key(sha256.New, password, salt[:], scramIterations, sha256.Size)
	if err != nil {
		return err
	}
	clientKey := scramHMAC(salted, "Client Key")
	storedKey := sha256.Sum256(clientKey)
	authMessage := clientFirstBare + "," + serverFirst + "," + withoutProof
	clientSig := scramHMAC(storedKey[:], authMessage)
	// The proof is ClientKey XOR ClientSignature; recover the key and check it.
	recovered := make([]byte, sha256.Size)
	for j := range recovered {
		recovered[j] = proof[j] ^ clientSig[j]
	}
	check := sha256.Sum256(recovered)
	if subtle.ConstantTimeCompare(check[:], storedKey[:]) != 1 {
		return errSCRAM
	}

	serverSig := scramHMAC(scramHMAC(salted, "Server Key"), authMessage)
	final := "v=" + base64.StdEncoding.EncodeToString(serverSig)
	c.writeMsg('R', append(be32(12), final...)) // AuthenticationSASLFinal
	return nil
}

func scramHMAC(key []byte, msg string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(msg))
	return h.Sum(nil)
}

// scramAttr returns the value of attribute name in a comma-separated SCRAM
// message, e.g. 'r' in "n=,r=abc".
func scramAttr(msg string, name byte) string {
	for _, kv := range strings.Split(msg, ",") {
		if len(kv) >= 2 && kv[0] == name && kv[1] == '=' {
			return kv[2:]
		}
	}
	return ""
}
