package stream

type commandVersion interface {
	GetMinVersion() uint16
	GetMaxVersion() uint16
	GetCommandKey() uint16
}

type commandVersionResponse struct {
	minVersion uint16
	maxVersion uint16
	commandKey uint16
}

func (c commandVersionResponse) GetMinVersion() uint16 {
	return c.minVersion
}

func (c commandVersionResponse) GetMaxVersion() uint16 {
	return c.maxVersion
}

func (c commandVersionResponse) GetCommandKey() uint16 {
	return c.commandKey
}

func newCommandVersionResponse(minVersion, maxVersion, commandKey uint16) commandVersionResponse {
	return commandVersionResponse{
		minVersion: minVersion,
		maxVersion: maxVersion,
		commandKey: commandKey,
	}
}

type PublishFilter struct {
}

func (p PublishFilter) GetMinVersion() uint16 {
	return version1
}

func (p PublishFilter) GetMaxVersion() uint16 {
	return version2
}

func (p PublishFilter) GetCommandKey() uint16 {
	return commandPublish
}
