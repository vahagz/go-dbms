package parser

func QueryDivider(data []byte, atEOF bool) (advance int, token []byte, err error) {
	// Return nothing if at end of file and no data passed
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}

	// // Find the index of the input of a newline followed by a pound sign.
	// if i := strings.Index(string(data), "\n#"); i >= 0 {
	// 	return i + 1, data[0:i], nil
	// }
	var lastQuoteChar byte = 0
	nextCharEscaped := false
	inQuoteScope := false
	for i, b := range data {
		if inQuoteScope && nextCharEscaped {
			nextCharEscaped = false
			continue
		}
		if inQuoteScope && b == '\\' {
			nextCharEscaped = true
			continue
		}

		if b == '\'' || b == '"' || b == '`' {
			if inQuoteScope && lastQuoteChar == b {
				inQuoteScope = false
				lastQuoteChar = 0
			} else {
				inQuoteScope = true
				lastQuoteChar = b
			}
		}

		if !inQuoteScope && b == ';' {
			return i + 1, data[0 : i+1], nil
		}
	}

	// If at end of file with data return the data
	if atEOF {
		data = append(data, ';')
		return len(data), data, nil
	}

	return
}
