package riakpbc

// Get server info
func (c *Client) GetServerInfo() (*RpbGetServerInfoResp, error) {
	reqdata := []byte{}

	response, err := c.ReqResp(reqdata, "RpbGetServerInfoReq", true)
	if err != nil {
		return nil, err
	}

	return response.(*RpbGetServerInfoResp), nil
}

// Ping the server
func (c *Client) Ping() ([]byte, error) {
	reqdata := []byte{}

	response, err := c.ReqResp(reqdata, "RpbPingReq", true)
	if err != nil {
		return nil, err
	}

	return response.([]byte), nil
}

// Get client ID
func (c *Client) GetClientId() (*RpbGetClientIdResp, error) {
	reqdata := []byte{}

	response, err := c.ReqResp(reqdata, "RpbGetClientIdReq", true)
	if err != nil {
		return nil, err
	}

	return response.(*RpbGetClientIdResp), nil
}

// Set client ID
func (c *Client) SetClientId(clientId string) ([]byte, error) {
	reqstruct := &RpbSetClientIdReq{
		ClientId: []byte(clientId),
	}

	response, err := c.ReqResp(reqstruct, "RpbSetClientIdReq", false)
	if err != nil {
		return nil, err
	}

	return response.([]byte), nil
}
