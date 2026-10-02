package ha

import "testing"

func TestBackOffConfigurationValidate(t *testing.T) {
	cases := []struct {
		name    string
		cfg     *BackOffConfiguration
		wantErr bool
	}{
		{"default", NewBackOffConfiguration(), false},
		{"nil", nil, true},
		{"min too low", &BackOffConfiguration{MinInterval: 0, MaxInterval: 8}, true},
		{"max too high", &BackOffConfiguration{MinInterval: 1, MaxInterval: 101}, true},
		{"max less than min", &BackOffConfiguration{MinInterval: 5, MaxInterval: 4}, true},
		{"boundaries", &BackOffConfiguration{MinInterval: 1, MaxInterval: 100}, false},
	}
	for _, c := range cases {
		if err := c.cfg.Validate(); (err != nil) != c.wantErr {
			t.Errorf("%s: unexpected result, err=%v", c.name, err)
		}
	}
}

func TestRandomWaitWithBackoff(t *testing.T) {
	cfg := &BackOffConfiguration{MinInterval: 2, MaxInterval: 4}
	if got := randomWaitWithBackoff(1, cfg); got != 2_000 {
		t.Errorf("without random expected 2000, got %d", got)
	}
	cfg.EnableRandom = true
	if got := randomWaitWithBackoff(1, cfg); got < 2_000 || got >= 6_000 {
		t.Errorf("out of range: %d", got)
	}
}
