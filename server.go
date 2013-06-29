package riakpbc

// Get server info
func (node *Node) GetServerInfo() (*RpbGetServerInfoResp, error) {
	reqdata := []byte{}

	response, err := node.ReqResp(reqdata, "RpbGetServerInfoReq", true)
	if err != nil {
		return nil, err
	}

	return response.(*RpbGetServerInfoResp), nil
}

// Ping the server
func (node *Node) Ping() ([]byte, error) {
	reqdata := []byte{}

	response, err := node.ReqResp(reqdata, "RpbPingReq", true)
	if err != nil {
		return nil, err
	}

	return response.([]byte), nil
}

// Get client ID
func (node *Node) GetClientId() (*RpbGetClientIdResp, error) {
	reqdata := []byte{}

	response, err := node.ReqResp(reqdata, "RpbGetClientIdReq", true)
	if err != nil {
		return nil, err
	}

	return response.(*RpbGetClientIdResp), nil
}

// Set client ID
func (node *Node) SetClientId(clientId string) ([]byte, error) {
	reqstruct := &RpbSetClientIdReq{
		ClientId: []byte(clientId),
	}

	response, err := node.ReqResp(reqstruct, "RpbSetClientIdReq", false)
	if err != nil {
		return nil, err
	}

	return response.([]byte), nil
}
