package amqp

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("AMQP 1.0 Message test", func() {
	Context("Encode and Decode", func() {
		It("Validate MarshalBinary and UnmarshalBinary standard", func() {
			longString := "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Donec a diam lectus. Sed sit amet ipsum mauris. " +
				"Maecenas congue ligula ac quam viverra nec consectetur ante hendrerit. Donec et mollis dolor. " +
				"Praesent et diam eget libero egestas mattis sit amet vitae augue. Nam tincidunt congue enim, " +
				"ut porta lorem lacinia consectetur. Donec ut libero sed arcu vehicula ultricies a non tortor. " +
				"Lorem ipsum dolor sit amet, consectetur adipiscing elit. Aenean ut gravida lorem. "
			message := NewAMQP10Message([]byte(longString))
			Expect(message.Data).To(Equal([]byte(longString)))
			message.ApplicationProperties = map[string]any{
				"test":  "test",
				"test2": 234,
			}
			message.DeliveryAnnotations = Annotations{
				"My_Annotation_1": "My_Annotation_1_value",
				"My_Annotation_2": int64(6789),
			}
			Expect(message.DeliveryAnnotations["My_Annotation_1"]).To(Equal("My_Annotation_1_value"))
			Expect(message.DeliveryAnnotations["My_Annotation_2"]).To(Equal(int64(6789)))
			Expect(message.ApplicationProperties["test"]).To(Equal("test"))
			Expect(message.ApplicationProperties["test2"]).To(Equal(234))
			message.Properties = &MessageProperties{
				ContentType: "application/json",
				Subject:     "test1",
				UserID:      []byte{0x00, 0x01, 0x02},
			}
			Expect(message.Properties.ContentType).To(Equal("application/json"))
			Expect(message.Properties.Subject).To(Equal("test1"))
			Expect(message.Properties.UserID).To(Equal([]byte{0x00, 0x01, 0x02}))

			buff, err := message.MarshalBinary()
			Expect(err).To(BeNil())
			message2 := &Message{}
			err = message2.UnmarshalBinary(buff)
			Expect(err).To(BeNil())
			Expect(message2.Data).To(Equal([]byte(longString)))
			Expect(message2.ApplicationProperties["test"]).To(Equal("test"))
			Expect(message2.ApplicationProperties["test2"]).To(Equal(int64(234)))
			Expect(message2.Properties.ContentType).To(Equal("application/json"))
			Expect(message2.Properties.Subject).To(Equal("test1"))
			Expect(message2.Properties.UserID).To(Equal([]byte{0x00, 0x01, 0x02}))
			Expect(message2.DeliveryAnnotations["My_Annotation_1"]).To(Equal("My_Annotation_1_value"))
			Expect(message2.DeliveryAnnotations["My_Annotation_2"]).To(Equal(int64(6789)))
		})
		It("Validate MarshalBinary and UnmarshalBinary Unicode", func() {
			// I don't know chinese, so I just copied the first paragraph of the wikipedia page for Alan Turing
			// https://zh.wikipedia.org/wiki/%E8%89%BE%E4%BC%A6%C2%B7%E5%9B%BE%E7%81%B5
			chString := "艾伦·麦席森·图灵，OBE，FRS（英語：Alan Mathison Turing，又译阿兰·图灵，Turing也常翻譯成涂林或者杜林，1912年6月23日—1954年6月7日）是英国電腦科學家、数学家、邏輯學家、密码分析学家和理论生物学家，他被誉为计算机科学與人工智能之父。\n\n二次世界大战期间，「Hut 8」小组，负责德国海军密码分析。 期间他设计了一些加速破译德国密码的技术，包括改进波兰战前研制的机器Bombe，一种可以找到恩尼格玛密码机设置的机电机器。 图灵在破译截获的编码信息方面发挥了关键作用，使盟军能够在包括大西洋战役在内的许多重要交战中击败軸心國海軍，并因此帮助赢得了战争[3][4]。"
			message := NewAMQP10Message([]byte(chString))
			Expect(message.Data).To(Equal([]byte(chString)))
			buff, err := message.MarshalBinary()
			Expect(err).To(BeNil())
			message2 := &Message{}
			err = message2.UnmarshalBinary(buff)
			Expect(err).To(BeNil())
			Expect(message2.Data).To(Equal([]byte(chString)))
		})
	})
})
