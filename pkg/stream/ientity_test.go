package stream

import (
	"errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// fakeEntity is a test double that implements the iEntity interface.
// It is used to verify interface contract and behavior without a real Producer/Consumer.
type fakeEntity struct {
	id          uint8
	streamName  string
	name        string
	closeErr    error
	closeChan   chan Event
	closeCalled bool
}

func newFakeEntity(streamName, name string, id uint8) *fakeEntity {
	return &fakeEntity{
		id:         id,
		streamName: streamName,
		name:       name,
		closeChan:  make(chan Event, 1),
	}
}

func (f *fakeEntity) Close() error {
	f.closeCalled = true
	if f.closeChan != nil {
		close(f.closeChan)
	}
	return f.closeErr
}

func (f *fakeEntity) NotifyClose() ChannelClose {
	return f.closeChan
}

func (f *fakeEntity) GetStreamName() string {
	return f.streamName
}

func (f *fakeEntity) GetName() string {
	return f.name
}

func (f *fakeEntity) GetID() uint8 {
	return f.id
}

func (f *fakeEntity) setID(id uint8) {
	f.id = id
}

// Compile-time check that fakeEntity implements iEntity.
var _ iEntity = (*fakeEntity)(nil)

var _ = Describe("iEntity", func() {

	Describe("fakeEntity interface compliance", func() {
		It("implements the iEntity interface", func() {
			entity := newFakeEntity("my-stream", "my-entity", 1)
			// Assignment to interface type verifies implementation
			var _ iEntity = entity
			Expect(entity).NotTo(BeNil())
		})
	})

	Describe("GetStreamName", func() {
		It("returns the stream name", func() {
			entity := newFakeEntity("test-stream", "producer-1", 0)
			Expect(entity.GetStreamName()).To(Equal("test-stream"))
		})

		It("returns empty string when not set", func() {
			entity := newFakeEntity("", "consumer-1", 2)
			Expect(entity.GetStreamName()).To(Equal(""))
		})
	})

	Describe("GetName", func() {
		It("returns the entity name", func() {
			entity := newFakeEntity("stream", "my-producer", 1)
			Expect(entity.GetName()).To(Equal("my-producer"))
		})

		It("returns empty string when not set", func() {
			entity := newFakeEntity("stream", "", 0)
			Expect(entity.GetName()).To(Equal(""))
		})
	})

	Describe("GetID and setID", func() {
		It("returns the initial id", func() {
			entity := newFakeEntity("stream", "entity", 5)
			Expect(entity.GetID()).To(Equal(uint8(5)))
		})

		It("updates id when setID is called", func() {
			entity := newFakeEntity("stream", "entity", 0)
			Expect(entity.GetID()).To(Equal(uint8(0)))
			entity.setID(42)
			Expect(entity.GetID()).To(Equal(uint8(42)))
		})

		It("handles id boundary values", func() {
			entity := newFakeEntity("stream", "entity", 0)
			entity.setID(254)
			Expect(entity.GetID()).To(Equal(uint8(254)))
		})
	})

	Describe("NotifyClose", func() {
		It("returns a channel that can receive close events", func() {
			entity := newFakeEntity("stream", "entity", 0)
			ch := entity.NotifyClose()
			Expect(ch).NotTo(BeNil())
			// Channel should be open before Close()
			select {
			case <-ch:
				Fail("channel should not receive before Close")
			default:
				// expected: channel not closed yet
			}
		})

		It("closes the channel when Close is called", func() {
			entity := newFakeEntity("stream", "entity", 0)
			ch := entity.NotifyClose()
			err := entity.Close()
			Expect(err).NotTo(HaveOccurred())
			_, ok := <-ch
			Expect(ok).To(BeFalse(), "channel should be closed after Close()")
		})
	})

	Describe("Close", func() {
		It("returns nil when no error is set", func() {
			entity := newFakeEntity("stream", "entity", 0)
			err := entity.Close()
			Expect(err).NotTo(HaveOccurred())
			Expect(entity.closeCalled).To(BeTrue())
		})

		It("returns the configured error when set", func() {
			closeErr := errors.New("close failed")
			entity := newFakeEntity("stream", "entity", 0)
			entity.closeErr = closeErr
			err := entity.Close()
			Expect(err).To(MatchError(closeErr))
			Expect(entity.closeCalled).To(BeTrue())
		})

		It("can be used as iEntity when closing", func() {
			entity := newFakeEntity("stream", "entity", 0)
			var e iEntity = entity
			err := e.Close()
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Describe("iEntity used polymorphically", func() {
		It("treats multiple fake entities via interface", func() {
			entities := []iEntity{
				newFakeEntity("stream-a", "producer-1", 0),
				newFakeEntity("stream-a", "consumer-1", 1),
				newFakeEntity("stream-b", "producer-2", 2),
			}
			Expect(entities).To(HaveLen(3))
			Expect(entities[0].GetStreamName()).To(Equal("stream-a"))
			Expect(entities[0].GetName()).To(Equal("producer-1"))
			Expect(entities[0].GetID()).To(Equal(uint8(0)))
			Expect(entities[2].GetStreamName()).To(Equal("stream-b"))
			Expect(entities[2].GetID()).To(Equal(uint8(2)))
			for _, e := range entities {
				Expect(e.Close()).NotTo(HaveOccurred())
			}
		})
	})
})
