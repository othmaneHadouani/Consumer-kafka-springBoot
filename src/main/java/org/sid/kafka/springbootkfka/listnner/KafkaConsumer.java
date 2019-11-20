package org.sid.kafka.springbootkfka.listnner;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.sid.kafka.springbootkfka.model.User;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumer{

    @KafkaListener(topics = "myTopic",groupId = "group_id")
    public void consume(ConsumerRecord<String,String> message){ // or we can use ConsumerRecord<String,String> cr ==> cr.value() and cr.key()        System.out.println(message);
        try {

            User user = this.getUserFromJson(message.value());
            System.out.println("key== "+message.key()+"user==" +user.toString());
        }
        catch (Exception e){
            e.printStackTrace();
        }

    }

  /* @KafkaListener(topics = "myTopic",groupId = "group_json",containerFactory = "userkafkaListenerContainerFactory")
    public void consumeJson(User user){
        System.out.println(user.toString());
    }*/

  private User getUserFromJson(String userJson)throws Exception{

      ObjectMapper objectMapper =new ObjectMapper();

      return objectMapper.readValue(userJson,User.class);

  }

}
