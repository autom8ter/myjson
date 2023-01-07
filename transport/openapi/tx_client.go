package openapi

import (
	"context"
	"net/http"
	"strings"

	"github.com/autom8ter/myjson/errors"
	"github.com/gorilla/websocket"
)

type TxClient struct {
	serverURL string
}

func NewTxClient(serverURL string) *TxClient {
	if strings.Contains(serverURL, "http") {
		serverURL = strings.Replace(serverURL, "http", "ws", 1)
	}
	return &TxClient{serverURL: serverURL}
}

func (c *TxClient) NewTx(header http.Header) (*TxSocket, error) {
	conn, _, err := websocket.DefaultDialer.Dial(c.serverURL+"/api/tx", header)
	if err != nil {
		return nil, errors.Wrap(err, http.StatusBadRequest, "failed to connect to tx websocket")
	}
	return &TxSocket{
		conn: conn,
	}, nil
}

type TxSocket struct {
	conn *websocket.Conn
}

func (t *TxSocket) Write(ctx context.Context, input TxInput) error {
	if err := t.conn.WriteJSON(&input); err != nil {
		return err
	}
	return nil
}

func (t *TxSocket) Read(ctx context.Context) (TxOutput, error) {
	if ctx.Err() != nil {
		return TxOutput{}, nil
	}
	var output TxOutput
	if err := t.conn.ReadJSON(&output); err != nil {
		return TxOutput{}, err
	}
	return output, nil
}

func (t *TxSocket) Close() error {
	t.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	return t.conn.Close()
}
