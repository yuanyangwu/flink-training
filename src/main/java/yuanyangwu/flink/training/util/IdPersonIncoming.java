package yuanyangwu.flink.training.util;

public class IdPersonIncoming {
    public int id;
    public PersonIncoming personIncoming;

    public IdPersonIncoming() {
        id = 0;
        personIncoming = new PersonIncoming();
    }

    public IdPersonIncoming(int id, PersonIncoming personIncoming) {
        this.id = id;
        this.personIncoming = personIncoming;
    }

    @Override
    public int hashCode() {
        int result = personIncoming != null ? personIncoming.hashCode() : 0;
        result = 31 * result + id;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof IdPersonIncoming)) {
            return false;
        }
        @SuppressWarnings("rawtypes")
        IdPersonIncoming value = (IdPersonIncoming) obj;
        if (personIncoming != null ? !personIncoming.equals(value.personIncoming) : value.personIncoming != null) {
            return false;
        }
        return id == value.id;
    }

    @Override
    public String toString() {
        return "{id=" + id + ", " + personIncoming.toString() + "}";
    }
}
