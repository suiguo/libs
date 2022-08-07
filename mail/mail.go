package mail

import (
	"crypto/tls"
	"ngin/global"
	"ngin/libs/logger"
	"sync"

	"gopkg.in/gomail.v2"
)

var once sync.Once
var d *gomail.Dialer
var msg chan *gomail.Message

func SendMail(address string, code string) {
	if d == nil {
		d = gomail.NewDialer("smtp.sina.cn", 465, "17612199113@sina.com", "5c8bb05583017daf")
		d.TLSConfig = &tls.Config{InsecureSkipVerify: true}
	}
	newMail := gomail.NewMessage()
	newMail.SetHeader("From", "17612199113@sina.com")
	newMail.SetHeader("To", address)
	newMail.SetAddressHeader("Cc", address, "")
	newMail.SetHeader("Subject", "Test")
	newMail.SetBody("text/html", "<b>[</b><i>"+code+"</i><b>]</b>")
	global.GLog.Info("info", "code", code)
	go func(mail *gomail.Message) {
		msg <- mail
	}(newMail)
}

func Work(log logger.Logger) {
	if msg == nil {
		msg = make(chan *gomail.Message, 10)
	}
	once.Do(func() {
		go func() {
			for {
				mail := <-msg
				if d != nil {
					if err := d.DialAndSend(mail); err != nil {
						if log != nil {
							log.Error("MailError", "error", err)
						}
					}
				}
			}
		}()
	})
}

// Use the channel in your program to send emails.

// Close the channel to stop the mail daemon.
// close(ch)
