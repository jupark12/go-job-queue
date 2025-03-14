package audio

import (
	"fmt"
	"math"
	"os"

	"github.com/go-audio/audio"
	"github.com/go-audio/wav"
)

// ProcessAudioForWhisper converts various audio formats to the normalized float32 array
// expected by whisper.cpp (16kHz mono PCM)
func ProcessAudioForWhisper(filename string) ([]float32, error) {
	// Open the audio file
	f, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open audio file: %w", err)
	}
	defer f.Close()

	// Determine file format and use appropriate decoder
	var pcmData []int
	var srcSampleRate int
	var bitDepth int

	if isWavFile(filename) {
		// Process WAV format
		decoder := wav.NewDecoder(f)
		decoder.ReadInfo()
		srcSampleRate = int(decoder.SampleRate)
		bitDepth = int(decoder.BitDepth)

		buffer, err := decoder.FullPCMBuffer()
		if err != nil {
			return nil, fmt.Errorf("failed to decode WAV data: %w", err)
		}

		// Convert to mono if stereo
		if buffer.Format.NumChannels > 1 {
			buffer = convertToMono(buffer)
		}

		pcmData = buffer.Data
	} else if isMp3File(filename) {
		// For MP3, you'd use a library like minimp3 or rely on ffmpeg
		// This would require CGO or external process execution
		return nil, fmt.Errorf("MP3 processing requires additional dependencies")
	} else {
		return nil, fmt.Errorf("unsupported audio format")
	}

	// Resample to 16kHz if needed
	if srcSampleRate != 16000 {
		pcmData = resamplePCM(pcmData, srcSampleRate, 16000)
	}

	// Convert PCM integer data to normalized float32 [-1.0, 1.0]
	floatData := make([]float32, len(pcmData))
	normalizationFactor := float32(math.Pow(2, float64(bitDepth-1)))

	for i, sample := range pcmData {
		floatData[i] = float32(sample) / normalizationFactor
	}

	return floatData, nil
}

// Convert multi-channel audio to mono by averaging channels
func convertToMono(buffer *audio.IntBuffer) *audio.IntBuffer {
	if buffer.Format.NumChannels == 1 {
		return buffer
	}

	numChannels := buffer.Format.NumChannels
	numSamples := len(buffer.Data) / numChannels
	monoData := make([]int, numSamples)

	for i := 0; i < numSamples; i++ {
		sum := 0
		for ch := 0; ch < numChannels; ch++ {
			sum += buffer.Data[i*numChannels+ch]
		}
		monoData[i] = sum / numChannels
	}

	return &audio.IntBuffer{
		Format: &audio.Format{
			NumChannels: 1,
			SampleRate:  buffer.Format.SampleRate,
		},
		Data: monoData,
	}
}

// Implement sample rate conversion
// Note: This is a simplified approach. Production code should use a proper resampling algorithm
func resamplePCM(pcmData []int, srcRate, dstRate int) []int {
	ratio := float64(dstRate) / float64(srcRate)
	outputLength := int(float64(len(pcmData)) * ratio)
	output := make([]int, outputLength)

	for i := range output {
		srcIdx := float64(i) / ratio
		idx1 := int(srcIdx)
		if idx1 >= len(pcmData)-1 {
			output[i] = pcmData[len(pcmData)-1]
			continue
		}

		frac := srcIdx - float64(idx1)
		output[i] = int(float64(pcmData[idx1])*(1-frac) + float64(pcmData[idx1+1])*frac)
	}

	return output
}

// Simple file type detection
func isWavFile(filename string) bool {
	return hasExtension(filename, ".wav")
}

func isMp3File(filename string) bool {
	return hasExtension(filename, ".mp3")
}

func hasExtension(filename, ext string) bool {
	l := len(filename)
	return l >= len(ext) && filename[l-len(ext):] == ext
}
