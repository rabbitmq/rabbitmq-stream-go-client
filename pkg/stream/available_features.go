package stream

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
)

var lock = &sync.Mutex{}

type availableFeatures struct {
	is313OrMore                       bool
	is311OrMore                       bool
	brokerFilterEnabled               bool
	brokerVersion                     string
	brokerSingleActiveConsumerEnabled bool
}

func newAvailableFeatures() *availableFeatures {
	lock.Lock()
	defer lock.Unlock()
	return &availableFeatures{}
}

func (a *availableFeatures) Is311OrMore() bool {
	lock.Lock()
	defer lock.Unlock()
	return a.is311OrMore
}

func (a *availableFeatures) Is313OrMore() bool {
	lock.Lock()
	defer lock.Unlock()
	return a.is313OrMore
}

func (a *availableFeatures) BrokerFilterEnabled() bool {
	lock.Lock()
	defer lock.Unlock()
	return a.brokerFilterEnabled
}

func (a *availableFeatures) IsBrokerSingleActiveConsumerEnabled() bool {
	lock.Lock()
	defer lock.Unlock()
	return a.brokerSingleActiveConsumerEnabled
}

func (a *availableFeatures) SetVersion(version string) error {
	lock.Lock()
	defer lock.Unlock()
	if extractVersion(version) == "" {
		return fmt.Errorf("invalid version format: %s", version)
	}
	a.brokerVersion = version
	a.is311OrMore = IsVersionGreaterOrEqual(extractVersion(version), "3.11.0")
	a.is313OrMore = IsVersionGreaterOrEqual(extractVersion(version), "3.13.0")
	a.brokerSingleActiveConsumerEnabled = a.is311OrMore
	return nil
}

func (a *availableFeatures) GetCommands() []commandVersion {
	lock.Lock()
	defer lock.Unlock()
	return []commandVersion{
		&PublishFilter{},
	}
}

func (a *availableFeatures) ParseCommandVersions(commandVersions []commandVersion) {
	lock.Lock()
	defer lock.Unlock()
	for _, commandVersion := range commandVersions {
		if commandVersion.GetCommandKey() == commandPublish {
			a.brokerFilterEnabled = commandVersion.GetMinVersion() <= PublishFilter{}.GetMinVersion() &&
				commandVersion.GetMaxVersion() >= PublishFilter{}.GetMaxVersion()
		}
	}
}

func (a *availableFeatures) String() string {
	return fmt.Sprintf("brokerVersion: %s, is311OrMore: %t, is313OrMore: %t, brokerFilterEnabled: %t", a.brokerVersion, a.is311OrMore, a.is313OrMore, a.brokerFilterEnabled)
}

func extractVersion(fullVersion string) string {
	pattern := `(\d+\.\d+\.\d+)`
	regex := regexp.MustCompile(pattern)
	match := regex.FindStringSubmatch(fullVersion)

	if len(match) > 1 {
		return match[1]
	}
	return ""
}

func IsVersionGreaterOrEqual(version, target string) bool {
	v1, err := parseVersion(version)
	if err != nil {
		return false
	}

	v2, err := parseVersion(target)
	if err != nil {
		return false
	}
	return v1.Compare(v2) >= 0
}

func parseVersion(version string) (Version, error) {
	parts := strings.Split(version, ".")
	if len(parts) != 3 {
		return Version{}, fmt.Errorf("invalid version format: %s", version)
	}

	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return Version{}, fmt.Errorf("invalid major version: %s", parts[0])
	}

	minor, err := strconv.Atoi(parts[1])
	if err != nil {
		return Version{}, fmt.Errorf("invalid minor version: %s", parts[1])
	}

	patch, err := strconv.Atoi(parts[2])
	if err != nil {
		return Version{}, fmt.Errorf("invalid patch version: %s", parts[2])
	}

	return Version{Major: major, Minor: minor, Patch: patch}, nil
}

type Version struct {
	Major int
	Minor int
	Patch int
}

func (v Version) Compare(other Version) int {
	if v.Major != other.Major {
		return v.Major - other.Major
	}
	if v.Minor != other.Minor {
		return v.Minor - other.Minor
	}
	return v.Patch - other.Patch
}
