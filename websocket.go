package bitfinex

import (
	"code.google.com/p/go.net/websocket"
	"encoding/json"
	"log"
	"strconv"
	//"reflect"
	"time"
)

// Pairs available
const (
	// Pairs
	BTCUSD = "BTCUSD"
	LTCUSD = "LTCUSD"
	LTCBTC = "LTCBTC"

	// Channels
	chanBook    = "book"
	chanTrades  = "trades"
	chanTicker  = "ticker"
	chanAccount = "account"
)

const (
	BITFINEX_WEBSOCKET                   = "wss://api2.bitfinex.com:3000/ws"
	BITFINEX_WEBSOCKET_VERSION           = "1.0"
	BITFINEX_WEBSOCKET_POSITION_SNAPSHOT = "ps"
	BITFINEX_WEBSOCKET_POSITION_NEW      = "pn"
	BITFINEX_WEBSOCKET_POSITION_UPDATE   = "pu"
	BITFINEX_WEBSOCKET_POSITION_CLOSE    = "pc"
	BITFINEX_WEBSOCKET_WALLET_SNAPSHOT   = "ws"
	BITFINEX_WEBSOCKET_WALLET_UPDATE     = "wu"
	BITFINEX_WEBSOCKET_ORDER_SNAPSHOT    = "os"
	BITFINEX_WEBSOCKET_ORDER_NEW         = "on"
	BITFINEX_WEBSOCKET_ORDER_UPDATE      = "ou"
	BITFINEX_WEBSOCKET_ORDER_CANCEL      = "oc"
	BITFINEX_WEBSOCKET_TRADE_EXECUTED    = "te"
)

type WebsocketBook struct {
	Price  float64
	Count  int
	Amount float64
}

type WebsocketTrade struct {
	ID        string
	Timestamp time.Time
	Price     float64
	Amount    float64
}

type WebsocketTicker struct {
	Bid             float64
	BidSize         float64
	Ask             float64
	AskSize         float64
	DailyChange     float64
	DailyChangePerc float64
	LastPrice       float64
	Volume          float64
}

type WebsocketTermData struct {
	T string
	E error
}

func (t WebsocketTermData) Term() string {
	return t.T
}

func (t WebsocketTermData) GetError() error {
	return t.E
}

type WebsocketTerm interface {
	Term() string
	GetError() error
}

type WebsocketPosition struct {
	Pair              string
	Status            string
	Amount            float64
	Price             float64
	MarginFunding     float64
	MarginFundingType int
	WebsocketTermData
}

type WebsocketWallet struct {
	Name              string
	Currency          string
	Balance           float64
	UnsettledInterest float64
	WebsocketTermData
}

type WebsocketOrder struct {
	OrderID    int64
	Pair       string
	Amount     float64
	AmountOrig float64
	Type       string
	Status     string
	Price      float64
	PriceAvg   float64
	Timestamp  time.Time
	Notify     int
	WebsocketTermData
}

type WebsocketTradeExecuted struct {
	TradeID        int64
	Pair           string
	Timestamp      int64
	OrderID        int64
	AmountExecuted float64
	PriceExecuted  float64
	WebsocketTermData
}

// WebSocketService allow to connect and receive stream data
// from bitfinex.com ws service.
type WebSocketService struct {
	// http client
	client *Client
	// websocket client
	ws *websocket.Conn
	// map internal channels to websocket's
	subChan map[int]*subscription
	subReq  []*subscription
	subAuth chan<- WebsocketTerm
}

type subscription struct {
	Channel string
	Params  map[string]string
	Chan    interface{}
}

func NewWebSocketService(c *Client) *WebSocketService {
	return &WebSocketService{
		client:  c,
		subChan: make(map[int]*subscription),
		subReq:  make([]*subscription, 0),
	}
}

// Connect create new bitfinex websocket connection
func (w *WebSocketService) Connect() error {
	ws, err := websocket.Dial(WebSocketURL, "", "http://localhost/")
	if err != nil {
		return err
	}
	w.ws = ws
	return nil
}

// Close web socket connection
func (w *WebSocketService) Close() {
	w.ws.Close()
}

func (w *WebSocketService) Send(data interface{}) error {
	//log.Print("send", data)
	msg, err := json.Marshal(data)
	if err != nil {
		return err
	}
	_, err = w.ws.Write(msg)
	return err
}

func (w *WebSocketService) subscribeAdd(channel string, p map[string]string, ch interface{}) {
	w.subReq = append(w.subReq, &subscription{
		Channel: channel,
		Params:  p,
		Chan:    ch,
	})
}

func (w *WebSocketService) subscribeMatch(s *subscription, ev map[string]interface{}) bool {
	if s.Channel != ev["channel"].(string) {
		return false
	}
	for k, v := range s.Params {
		vv, ok := ev[k]
		if ok && v == vv.(string) {
			return true
		}
	}
	return false
}

func (w *WebSocketService) SubscribeTicker(pair string, ch chan<- WebsocketTicker) {
	w.subscribeAdd(chanTicker, map[string]string{
		"pair": pair,
	}, ch)
}

func (w *WebSocketService) SubscribeBook(pair string, precision string, ch chan<- WebsocketBook) {
	w.subscribeAdd(chanBook, map[string]string{
		"pair": pair,
		"prec": precision,
	}, ch)
}

func (w *WebSocketService) SubscribeTrades(pair string, ch chan<- WebsocketTrade) {
	w.subscribeAdd(chanTrades, map[string]string{
		"pair": pair,
	}, ch)
}

func (w *WebSocketService) SubscribeAccount(ch chan<- WebsocketTerm) {
	w.subAuth = ch
}

// Watch allows to subsribe to channels and watch for new updates.
// This method supports next channels: book, trade, ticker.
func (w *WebSocketService) Subscribe() error {
	// Subscribe to each channel
	for _, s := range w.subReq {
		request := map[string]string{
			"event":   "subscribe",
			"channel": s.Channel,
		}
		for k, v := range s.Params {
			request[k] = v
		}
		if err := w.Send(request); err != nil {
			return err
		}
	}

	if w.subAuth != nil {
		payload := "AUTH" + strconv.FormatInt(time.Now().UnixNano(), 10)[:13]
		request := map[string]string{
			"event":       "auth",
			"apiKey":      w.client.ApiKey,
			"authSig":     w.client.SignPayload(payload),
			"authPayload": payload,
		}

		err := w.Send(request)
		if err != nil {
			return err
		}
	}

	var clientMessage string
	for {
		if err := websocket.Message.Receive(w.ws, &clientMessage); err != nil {
			log.Fatal("Error reading message: ", err)
			continue
		}

		msg := []byte(clientMessage)

		// Check for first message(event:subscribed)
		var event map[string]interface{}
		if err := json.Unmarshal(msg, &event); err == nil {
			//log.Println("event", event)
			// Received "subscribed" resposne. Link channels.
			switch event["event"].(string) {
			case "subscribed":
				chanId := int(event["chanId"].(float64))
				for _, s := range w.subReq {
					if w.subscribeMatch(s, event) {
						//log.Printf("subscribed to chan '%s' on pair '%s', chanId=%v\n",
						//s.Channel, s.Params["pair"], chanId)
						w.subChan[chanId] = s
					}
				}
			case "auth":
				chanId := int(event["chanId"].(float64))
				status := event["status"].(string)
				if status == "OK" {
					//log.Println("subscribed to auth chan")
					w.subChan[chanId] = &subscription{
						Chan:   w.subAuth,
						Params: map[string]string{"channel": chanAccount},
					}
				} else if status == "FAILED" {
					log.Println("Websocket auth failed:", event["code"])
				}
			}
			continue
		}

		// Received snapshot or data update
		var data []interface{}
		if err := json.Unmarshal([]byte(clientMessage), &data); err != nil {
			log.Println("Error decoding data", err)
			continue
		}

		chanId := int(data[0].(float64))
		if chanId == 0 {
			chanInfo, ok := w.subChan[0]
			if !ok {
				log.Println("Error decoding data")
				continue
			}
			//log.Printf("auth message %#v\n", data)
			ch := chanInfo.Chan.(chan<- WebsocketTerm)
			term := data[1].(string)
			data = data[2].([]interface{})
			if len(data) == 0 {
				continue
			}
			if _, ok := data[0].([]interface{}); ok {
				for _, v := range data {
					w.receiveAuthMessage(ch, term, v.([]interface{}))
				}
			} else {
				w.receiveAuthMessage(ch, term, data)
			}
		} else {
			chanInfo, ok := w.subChan[chanId]
			if !ok {
				log.Println("unexpected chanId=%d\n", chanId)
				continue
			}
			if items, ok := data[1].([]interface{}); ok {
				for _, v := range items {
					w.receivePublicMessage(chanInfo, v.([]interface{}))
				}
			} else {
				w.receivePublicMessage(chanInfo, data[1:])
			}
		}
	}
}

func (w *WebSocketService) receivePublicMessage(chanInfo *subscription, data []interface{}) {
	switch chanInfo.Channel {
	case chanBook:
		chanInfo.Chan.(chan<- WebsocketBook) <- WebsocketBook{
			Price:  data[0].(float64),
			Count:  int(data[1].(float64)),
			Amount: data[2].(float64),
		}
	case chanTrades:
		chanInfo.Chan.(chan<- WebsocketTrade) <- WebsocketTrade{
			ID:        data[0].(string),
			Timestamp: time.Unix(int64(data[1].(float64)), 0),
			Price:     data[2].(float64),
			Amount:    data[3].(float64),
		}
	case chanTicker:
		chanInfo.Chan.(chan<- WebsocketTicker) <- WebsocketTicker{
			Bid:             data[0].(float64),
			BidSize:         data[1].(float64),
			Ask:             data[2].(float64),
			AskSize:         data[3].(float64),
			DailyChange:     data[4].(float64),
			DailyChangePerc: data[5].(float64),
			LastPrice:       data[6].(float64),
			Volume:          data[7].(float64),
		}
	}
}

func (w *WebSocketService) receiveAuthMessage(ch chan<- WebsocketTerm, term string, data []interface{}) {

	//log.Printf("account %s %v\n", term, data)
	switch term {
	case "ps", "pn", "pu", "pc":
		ch <- WebsocketPosition{
			WebsocketTermData: WebsocketTermData{term, nil},
			Pair:              data[0].(string),
			Status:            data[1].(string),
			Amount:            data[2].(float64),
			Price:             data[3].(float64),
			MarginFunding:     data[4].(float64),
			MarginFundingType: int(data[5].(float64)),
		}
	case "ws", "wu":
		ch <- WebsocketWallet{
			WebsocketTermData: WebsocketTermData{term, nil},
			Name:              data[0].(string),
			Currency:          data[1].(string),
			Balance:           data[2].(float64),
			UnsettledInterest: data[3].(float64),
		}
	case "os", "on", "ou", "oc":
		t, _ := time.Parse("2006-01-02T15:04:05-07:00", data[8].(string))
		ch <- WebsocketOrder{
			OrderID:    int64(data[0].(float64)),
			Pair:       data[1].(string),
			Amount:     data[2].(float64),
			AmountOrig: data[3].(float64),
			Type:       data[4].(string),
			Status:     data[5].(string),
			Price:      data[6].(float64),
			PriceAvg:   data[7].(float64),
			Timestamp:  t,
		}
	}
}

/////////////////////////////
// Private websocket messages
/////////////////////////////
/*
// Private channel auth response
type privateResponse struct {
	Event  string  `json:"event"`
	Status string  `json:"status"`
	ChanId float64 `json:"chanId,omitempty"`
	UserId float64 `json:"userId"`
}

type TermData struct {
	// Data term. E.g: ps, ws, ou, etc... See official documentation for more details.
	Term string
	// Data will contain different number of elements for each term.
	// Examples:
	// Term: ws, Data: ["exchange","BTC",0.01410829,0]
	// Term: oc, Data: [0,"BTCUSD",0,-0.01,"","CANCELED",270,0,"2015-10-15T11:26:13Z",0]
	Data  []interface{}
	Error string
}

func (c *TermData) HasError() bool {
	return len(c.Error) > 0
}

func (w *WebSocketService) ConnectPrivate(ch chan TermData) {
	ws, err := websocket.Dial(WebSocketURL, "", "http://localhost/")
	if err != nil {
		ch <- TermData{
			Error: err.Error(),
		}
		return
	}

	// Send auth message

	var msg string
	for {
		if err = websocket.Message.Receive(ws, &msg); err != nil {
			ch <- TermData{
				Error: err.Error(),
			}
			ws.Close()
			return
		} else {
			event := &privateResponse{}
			err = json.Unmarshal([]byte(msg), &event)
			if err != nil {
				// received data update
				var data []interface{}
				err = json.Unmarshal([]byte(msg), &data)
				if err == nil {
					dataTerm := data[1].(string)
					dataList := data[2].([]interface{})

					// check for empty data
					if len(dataList) > 0 {
						if reflect.TypeOf(dataList[0]) == reflect.TypeOf([]interface{}{}) {
							// received list of lists
							for _, v := range dataList {
								ch <- TermData{
									Term: dataTerm,
									Data: v.([]interface{}),
								}
							}
						} else {
							// received flat list
							ch <- TermData{
								Term: dataTerm,
								Data: dataList,
							}
						}
					}
				}
			} else {
				// received auth response
				if event.Event == "auth" && event.Status != "OK" {
					ch <- TermData{
						Error: "Error connecting to private web socket channel.",
					}
					ws.Close()
				}
			}
		}
	}
}
*/
