package yuanyangwu.flink.training.element;

import org.apache.flink.api.java.tuple.Tuple2;

public class TupleBasedPersonIncoming extends Tuple2<String, Integer> {
    public TupleBasedPersonIncoming() {
        super();
    }

    public TupleBasedPersonIncoming(String person, Integer incoming) {
        super(person, incoming);
    }

    public void setPerson(String person) {
        this.f0 = person;
    }

    public String getPerson() {
        return this.f0;
    }

    public void setIncoming(Integer incoming) {
        this.f1 = incoming;
    }

    public Integer getIncoming() {
        return this.f1;
    }
}
