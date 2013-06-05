package riakpbc

// Get server info
func (c *Conn) GetServerInfo() (*RpbGetServerInfoResp, error) {
	reqdata := []byte{}

	err := c.RawRequest(reqdata, "RpbGetServerInfoReq")
	if err != nil {
		return &RpbGetServerInfoResp{}, err
	}

	response, err := c.Response(&RpbGetServerInfoResp{})
	if err != nil {
		return &RpbGetServerInfoResp{}, err
	}

	return response.(*RpbGetServerInfoResp), nil
}

// Ping the server
func (c *Conn) Ping() ([]byte, error) {
	reqdata := []byte{}

	if err := c.RawRequest(reqdata, "RpbPingReq"); err != nil {
		return nil, err
	}

	response, err := c.Response(&RpbEmptyResp{})
	if err != nil {
		return nil, err
	}

	return response.([]byte), nil
}

// Get client ID
func (c *Conn) GetClientId() (*RpbGetClientIdResp, error) {
	reqdata := []byte{}

	if err := c.RawRequest(reqdata, "RpbGetClientIdReq"); err != nil {
		return &RpbGetClientIdResp{}, err
	}

	response, err := c.Response(&RpbGetClientIdResp{})
	if err != nil {
		return &RpbGetClientIdResp{}, err
	}

	return response.(*RpbGetClientIdResp), nil
}

// Set client ID
func (c *Conn) SetClientId(clientId string) ([]byte, error) {
	reqstruct := &RpbSetClientIdReq{
		ClientId: []byte(clientId),
	}

	if err := c.Request(reqstruct, "RpbSetClientIdReq"); err != nil {
		return nil, err
	}

	response, err := c.Response(&RpbSetClientIdReq{})
	if err != nil {
		return nil, err
	}

	return response.([]byte), nil
}
