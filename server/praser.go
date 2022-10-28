package server

// option type
const (
	OP_START = iota
	OP_P
	OP_PU
	OP_PUB
	PUB_ARG
	MSG_PAYLOAD
	MSG_END
	OP_S
	OP_SU
	OP_SUB
)

func (c *client) prase(buf []byte) error {
	c.nb += len(buf)

	for i, b := range buf {
		switch c.state {
		case OP_START:
			switch b {
			case 'P', 'p':
				c.state = OP_P
			case 'S', 's':
				c.state = OP_S
			default:
				goto praseErr
			}
		case OP_P:
			switch b {
			case 'U', 'u':
				c.state = OP_PU
			default:
				goto praseErr
			}
		case OP_PU:
			switch b {
			case 'B', 'b':
				c.state = OP_PUB
			default:
				goto praseErr
			}
		case OP_PUB:
			switch b {
			case ' ', '\t':
				continue
			default:
				c.state = PUB_ARG
				c.as = i
			}
		case PUB_ARG:
			switch b {
			case '\r':
				c.drop = 1
			case '\n':
				var arg []byte
				if c.argBuf != nil {
					arg = c.argBuf
					c.argBuf = nil
				} else {
					arg = buf[c.as : i-c.drop]
				}
				if err := c.processPub(arg); err != nil {
					return err
				}
				c.drop, c.as, c.state = 0, i+1, MSG_PAYLOAD
			default:
				if c.argBuf != nil {
					c.argBuf = append(c.argBuf, b)
				}
			}
		case MSG_PAYLOAD:
			if c.msgBuf != nil {
				if len(c.msgBuf) >= c.pa.size {
					c.processMsg(c.msgBuf)
					c.msgBuf, c.state = nil, MSG_END
				} else {
					c.msgBuf = append(c.msgBuf, b)
				}
			} else if i-c.as >= c.pa.size {
				c.processMsg(buf[c.as:i])
				c.state = MSG_END
			}
		case MSG_END:
			switch b {
			case '\n':
				c.drop, c.as, c.state = 0, i+1, OP_START
			default:
				continue
			}
		}
	}
praseErr:
	return nil
}
