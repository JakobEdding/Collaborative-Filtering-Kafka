package de.hpi.collaborativefilteringkafka.messages;

public class IdRatingPairMessage {
    public int id;
    public short rating;

    public IdRatingPairMessage(int id, short rating) {
        this.id = id;
        this.rating = rating;
    }

    public String toString() {
        return String.format("Id: %d, Rating: %d", this.id, this.rating);
    }
}
