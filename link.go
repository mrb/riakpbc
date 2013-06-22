package riakpbc

// LinkAdd sets a link reference to the link bucket/key in bucket/key.
//
// Note that this can be manually done by passing RpbContent to StoreObject.
func (c *Client) LinkAdd(bucket, key, lbucket, lkey, ltag string) error {
	obj, err := c.FetchObject(bucket, key)
	if err != nil {
		return err
	}

	if len(obj.GetContent()) == 0 {
		return ErrNoContent
	}

	link := &RpbLink{
		Bucket: []byte(lbucket),
		Key:    []byte(lkey),
		Tag:    []byte(ltag),
	}
	obj.Content[0].Links = append(obj.Content[0].Links, link)

	if _, err := c.StoreObject(bucket, key, obj.GetContent()[0]); err != nil {
		return err
	}

	return nil
}

// LinkWalk is just a synonymn for FetchObject.  It expects the link bucket/key.
func (c *Client) LinkWalk(bucket, key string) (*RpbGetResp, error) {
	return c.FetchObject(bucket, key)
}

// LinkRemove removes the associated link bucket/key from the bucket/key.
func (c *Client) LinkRemove(bucket, key, lbucket, lkey string) error {
	obj, err := c.FetchObject(bucket, key)
	if err != nil {
		return err
	}

	if len(obj.GetContent()) == 0 {
		return ErrNoContent
	}

	for i, k := range obj.GetContent()[0].GetLinks() {
		if string(k.GetBucket()) == lbucket && string(k.GetKey()) == lkey {
			obj.Content[0].Links[i] = obj.Content[0].Links[len(obj.Content[0].Links)-1]
			obj.Content[0].Links = obj.Content[0].Links[0 : len(obj.Content[0].Links)-1]
			break
		}
	}

	if _, err := c.StoreObject(bucket, key, obj.GetContent()[0]); err != nil {
		return err
	}

	return nil
}
