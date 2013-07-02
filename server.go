package riakpbc

// Get server info
func (self *Node) GetServerInfo() (*RpbGetServerInfoResp, error) {
	reqdata := []byte{}

	response, err := self.ReqResp(reqdata, "RpbGetServerInfoReq", true)
	if err != nil {
		return nil, err
	}

	return response.(*RpbGetServerInfoResp), nil
}

// Ping the server
func (self *Node) Ping() ([]byte, error) {
	reqdata := []byte{}

	response, err := self.ReqResp(reqdata, "RpbPingReq", true)
	if err != nil {
		return nil, err
	}

	return response.([]byte), nil
}

// Get client ID
func (self *Node) GetClientId() (*RpbGetClientIdResp, error) {
	reqdata := []byte{}

	response, err := self.ReqResp(reqdata, "RpbGetClientIdReq", true)
	if err != nil {
		return nil, err
	}

	return response.(*RpbGetClientIdResp), nil
}

// Set client ID
func (self *Node) SetClientId(clientId string) ([]byte, error) {
	reqstruct := &RpbSetClientIdReq{
		ClientId: []byte(clientId),
	}

	response, err := self.ReqResp(reqstruct, "RpbSetClientIdReq", false)
	if err != nil {
		return nil, err
	}

	return response.([]byte), nil
}
