package riakpbc

// Get server info
func (c *Conn) GetServerInfo() (*RpbGetServerInfoResp, error) {
	reqdata := []byte{}

	node := c.SelectNode()

	err := node.RawRequest(reqdata, "RpbGetServerInfoReq")
	if err != nil {
		return &RpbGetServerInfoResp{}, err
	}

	response, err := node.Response()
	if err != nil {
		return &RpbGetServerInfoResp{}, err
	}

	return response.(*RpbGetServerInfoResp), nil
}

// Ping the server
func (c *Conn) Ping() ([]byte, error) {
	reqdata := []byte{}

	node := c.SelectNode()

	if err := node.RawRequest(reqdata, "RpbPingReq"); err != nil {
		return nil, err
	}

	response, err := node.Response()
	if err != nil {
		return nil, err
	}

	return response.([]byte), nil
}

// Get client ID
func (c *Conn) GetClientId() (*RpbGetClientIdResp, error) {
	reqdata := []byte{}

	node := c.SelectNode()

	if err := node.RawRequest(reqdata, "RpbGetClientIdReq"); err != nil {
		return &RpbGetClientIdResp{}, err
	}

	response, err := node.Response()
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

	node := c.SelectNode()

	response, err := node.ReqResp(reqstruct, "RpbSetClientIdReq")
	if err != nil {
		return nil, err
	}

	return response.([]byte), nil
}
