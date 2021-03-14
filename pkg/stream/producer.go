package stream

import (
	"bytes"
	"context"
	"github.com/Azure/go-amqp"
)

type Producer struct {
	ProducerID     uint8
	LikedClient    *Client
	PublishConfirm *Response
}

func (producer *Producer) BatchPublish(ctx context.Context, msgs []*amqp.Message) (int, error) {
	//respChan := make(chan *WriteResponse, 1)

	//go func(msgs []*amqp.Message) {
	frameHeaderLength := 2 + 2 + 1 + 4
	var msgLen int
	for _, msg := range msgs {
		r, _ := msg.MarshalBinary()
		msgLen += len(r) + 8 + 4
	}

	length := frameHeaderLength + msgLen
	var publishId uint8
	publishId = producer.ProducerID
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	WriteInt(b, length)
	WriteShort(b, CommandPublish)
	WriteUShort(b, Version1)
	WriteByte(b, publishId)
	WriteInt(b, len(msgs)) //toExcluded - fromInclude

	var seq int64
	seq = 0
	for _, msg := range msgs {
		r, _ := msg.MarshalBinary()
		WriteLong(b, seq)   // sequence
		WriteInt(b, len(r)) // len
		b.Write(r)
		seq += 1
	}

	err := producer.LikedClient.writeAndFlush(b.Bytes())
	if err != nil {
		return 0, err
	}
	//<-producer.PublishConfirm.isDone

	//select {
	//case _ = <-producer.PublishConfirm.isDone:
	//	return 0, nil
	//case <-time.After(200 * time.Millisecond):
	//	//fmt.Printf("timeout id:%d \n", producer.ProducerID)
	//}
	//producer.LikedClient.handleResponse()
	//respChan <- &WriteResponse{}
	//}(msgs)

	//select {
	//
	//case <-ctx.Done():
	//	switch ctx.Err() {
	//	case context.DeadlineExceeded:
	//		return 0, errors.Wrap(ctx.Err(), "Time out during send")
	//	case context.Canceled:
	//		return 0, errors.Wrap(ctx.Err(), "Call cancelled")
	//	}
	//case ree := <-respChan:
	//	return ree.Code, nil
	//
	//}
	return 0, nil
}

func (producer *Producer) Close() error {
	return producer.LikedClient.deletePublisher(producer.ProducerID)
}


func (client *Client) NewProducer(stream string) (*Producer, error) {
	return client.declarePublisher(stream)
}

func (client *Client) declarePublisher(stream string) (*Producer, error) {
	producer := client.producers.New()
	producer.LikedClient = client
	publisherReferenceSize := 0
	length := 2 + 2 + 4 + 1 + 2 + publisherReferenceSize + 2 + len(stream)
	resp := client.responses.New()
	correlationId := resp.subId
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	WriteInt(b, length)
	WriteShort(b, CommandDeclarePublisher)
	WriteShort(b, Version1)
	WriteInt(b, correlationId)
	WriteByte(b, producer.ProducerID)
	WriteShort(b, int16(publisherReferenceSize))
	WriteString(b, stream)
	err := client.writeAndFlush(b.Bytes())
	if err != nil {
		return nil, err
	}
	<-resp.code
	err = client.responses.RemoveById(correlationId)
	if err != nil {
		return nil, err
	}
	return producer, nil
}


func (client *Client) deletePublisher(publisherId byte) error {
	length := 2 + 2 + 4 + 1
	resp := client.responses.New()
	correlationId := resp.subId
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	WriteInt(b, length)
	WriteShort(b, CommandDeletePublisher)
	WriteShort(b, Version1)
	WriteInt(b, correlationId)
	WriteByte(b, publisherId)
	err := client.writeAndFlush(b.Bytes())
	if err != nil {
		return err
	}
	<-resp.code
	err = client.responses.RemoveById(correlationId)
	if err != nil {
		return err
	}
	err = client.producers.RemoveById(publisherId)
	if err != nil {
		return err
	}

	return nil
}


func (client *Client) CloseAllProducers() error {
	return client.producers.CloseAllProducers()
}

