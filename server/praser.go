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
	SUB_ARG
	OP_U
	OP_UN
	OP_UNS
	OP_UNSU
	OP_UNSUB
	UNSUB_ARG
	OP_PI
	OP_PIN
	OP_PING
	OP_C
	OP_CO
	OP_CON
	OP_CONN
	OP_CONNE
	OP_CONNEC
	OP_CONNECT
	CONNECT_ARG
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
			case 'U', 'u':
				c.state = OP_U
			case 'C', 'c':
				c.state = OP_C
			default:
				goto praseErr
			}
		case OP_P:
			switch b {
			case 'U', 'u':
				c.state = OP_PU
			case 'I', 'i':
				c.state = OP_PI
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
		case OP_S:
			switch b {
			case 'u', 'U':
				c.state = OP_SU
			default:
				goto praseErr
			}
		case OP_SU:
			switch b {
			case 'b', 'B':
				c.state = OP_SUB
			default:
				goto praseErr
			}
		case OP_SUB:
			switch b {
			case ' ', '\t':
				continue
			default:
				c.state = SUB_ARG
				c.as = i
			}
		case SUB_ARG:
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
				if err := c.processSub(arg); err != nil {
					return err
				}
				c.drop, c.as, c.state = 0, i+1, OP_START
			default:
				if c.argBuf != nil {
					c.argBuf = append(c.argBuf, b)
				}
			}
		case OP_U:
			switch b {
			case 'N', 'n':
				c.state = OP_UN
			default:
				goto praseErr
			}
		case OP_UN:
			switch b {
			case 'S', 's':
				c.state = OP_UNS
			default:
				goto praseErr
			}
		case OP_UNS:
			switch b {
			case 'u', 'U':
				c.state = OP_UNSU
			default:
				goto praseErr
			}
		case OP_UNSU:
			switch b {
			case 'B', 'b':
				c.state = OP_UNSUB
			default:
				goto praseErr
			}
		case OP_UNSUB:
			switch b {
			case ' ', '\t':
				continue
			default:
				c.as = i
				c.state = UNSUB_ARG
			}
		case UNSUB_ARG:
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
				if err := c.processUnsub(arg); err != nil {
					return err
				}
				c.drop, c.as, c.state = 0, i+1, OP_START
			default:
				if c.argBuf != nil {
					c.argBuf = append(c.argBuf, b)
				}
			}
		case OP_PI:
			switch b {
			case 'N', 'n':
				c.state = OP_PIN
			default:
				goto praseErr
			}
		case OP_PIN:
			switch b {
			case 'G', 'g':
				c.state = OP_PING
			default:
				goto praseErr
			}
		case OP_PING:
			switch b {
			case '\n':
				c.processPing()
				c.drop, c.as, c.state = 0, i+1, OP_START
			}
		case OP_C:
			switch b {
			case 'O', 'o':
				c.state = OP_CO
			default:
				goto praseErr
			}
		case OP_CO:
			switch b {
			case 'N', 'n':
				c.state = OP_CON
			default:
				goto praseErr
			}
		case OP_CON:
			switch b {
			case 'N', 'n':
				c.state = OP_CONN
			default:
				goto praseErr
			}
		case OP_CONN:
			switch b {
			case 'E', 'e':
				c.state = OP_CONNE
			default:
				goto praseErr
			}
		case OP_CONNE:
			switch b {
			case 'C', 'c':
				c.state = OP_CONNEC
			default:
				goto praseErr
			}
		case OP_CONNEC:
			switch b {
			case 'T', 't':
				c.state = OP_CONNECT
			default:
				goto praseErr
			}
		case OP_CONNECT:
			switch b {
			case ' ', '\t':
				continue
			default:
				c.as = i
				c.state = CONNECT_ARG
			}
		case CONNECT_ARG:
			switch b {
			case '\r':
				c.drop = 1
			case '\n':
				if err := c.processConnect(buf[c.as : i-c.drop]); err != nil {
					return err
				}
			default:
				goto praseErr
			}
		}

	}
praseErr:
	return nil
}
