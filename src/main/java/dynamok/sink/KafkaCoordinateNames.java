package dynamok.sink;

public class KafkaCoordinateNames {

    public final String topic;
    public final String partition;
    public final String offset;

    public KafkaCoordinateNames(String topic, String partition, String offset) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
    }

    @Override
    public String toString() {
        return "KafkaCoordinateNames{" +
                "topic='" + topic + '\'' +
                ", partition='" + partition + '\'' +
                ", offset='" + offset + '\'' +
                '}';
    }

}
