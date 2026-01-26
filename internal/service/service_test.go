package service

import (
	"strconv"
	"testing"
)

func TestExtractData(t *testing.T) {
	baseUrl := "https://sdmx.oecd.org/public/rest/data"
	tests := []struct {
		name      string
		url       string
		indicator string
		expectErr bool
	}{
		{
			name:      "Test 1",
			url:       "OECD.ELS.SAE,DSD_POPULATION@DF_POP_HIST,/.POP.PS._T..",
			indicator: "Population",
			expectErr: false,
		},
		{
			name:      "Test 2",
			url:       "",
			indicator: "Health",
			expectErr: true,
		},
		{
			name:      "Test 3",
			url:       " OECD.ELS.SAE,DSD_",
			indicator: "",
			expectErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			formattedUrl, err := ConstructOEDC_URL(baseUrl, tt.url, strconv.Itoa(CURRENT_YEAR-16))

			_, err = ExtractData(formattedUrl, tt.indicator)
			if (err != nil) != tt.expectErr {
				t.Errorf("expected error: %v, got: %v", tt.expectErr, err)
			}
		})
	}
}

func TestTransformData(t *testing.T) {

	baseUrl := "https://sdmx.oecd.org/public/rest/data"
	tests := []struct {
		name      string
		url       string
		indicator string
		expectErr bool
	}{
		{
			name:      "Test 1",
			url:       "OECD.ELS.SAE,DSD_POPULATION@DF_POP_HIST,/.POP.PS._T..",
			indicator: "Population",
			expectErr: false,
		},
		{
			name:      "Test 2",
			url:       "OECD.ELS.SAE,DSD_POPULATION@DF_POP_HIST,/.POP.PS._T..",
			indicator: "Popu",
			expectErr: true,
		},
		{
			name:      "Test 3",
			url:       "OECD.ELS.SAE,DSD_POPULATION@DF_POP_HIST,/.POP.PS._T..",
			indicator: "",
			expectErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			formattedUrl, err := ConstructOEDC_URL(baseUrl, tt.url, strconv.Itoa(CURRENT_YEAR-16))
			data, err := ExtractData(formattedUrl, tt.indicator)
			_, err = TransformData(data, tt.indicator)
			// fmt.Println(data)
			if (err != nil) != tt.expectErr {
				t.Errorf("expected error: %v, got: %v", tt.expectErr, err)
			}
		})
	}
}
