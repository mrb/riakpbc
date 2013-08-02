package riakpbc

// GetServerInfo returns the server info.
func (c *Client) GetServerInfo() (*RpbGetServerInfoResp, error) {
	opts := []byte{}

	response, err := c.ReqResp(opts, "RpbGetServerInfoReq", true)
	if err != nil {
		return nil, err
	}

	return response.(*RpbGetServerInfoResp), nil
}

// Ping the server.
func (c *Client) Ping() ([]byte, error) {
	opts := []byte{}

	response, err := c.ReqResp(opts, "RpbPingReq", true)
	if err != nil {
		return nil, err
	}

	return response.([]byte), nil
}

// GetClientId returns the client ID.
func (c *Client) GetClientId() (*RpbGetClientIdResp, error) {
	opts := []byte{}

	response, err := c.ReqResp(opts, "RpbGetClientIdReq", true)
	if err != nil {
		return nil, err
	}

	return response.(*RpbGetClientIdResp), nil
}

// NewSetClientIdRequest prepares a new SetClientId request.
func (c *Client) NewSetClientIdRequest(clientId string) *RpbSetClientIdReq {
	return &RpbSetClientIdReq{
		ClientId: []byte(clientId),
	}
}

func (c *Client) setClientId(opts *RpbSetClientIdReq, clientId string) ([]byte, error) {
	if opts == nil {
		opts = c.NewSetClientIdRequest(clientId)
	}

	response, err := c.ReqResp(opts, "RpbSetClientIdReq", false)
	if err != nil {
		return nil, err
	}

	return response.([]byte), nil
}

// SetClientId sets the client ID.
func (c *Client) SetClientId(clientId string) ([]byte, error) {
	return c.setClientId(nil, clientId)
}
