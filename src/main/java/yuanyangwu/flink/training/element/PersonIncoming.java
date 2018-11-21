package yuanyangwu.flink.training.element;

public class PersonIncoming {
    public String person;
    public int incoming;

    public PersonIncoming() {
        person = "";
        incoming = 0;
    }

    public PersonIncoming(String person, int incoming) {
        this.person = person;
        this.incoming = incoming;
    }

    public String getPerson() {
        return person;
    }

    public void setPerson(String person) {
        this.person = person;
    }

    public int getIncoming() {
        return incoming;
    }

    public void setIncoming(int incoming) {
        this.incoming = incoming;
    }

    @Override
    public String toString() {
        return "{person=" + person + ", incoming=" + incoming + "}";
    }

    @Override
    public int hashCode() {
        int result = person != null ? person.hashCode() : 0;
        result = 31 * result + incoming;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof PersonIncoming)) {
            return false;
        }
        @SuppressWarnings("rawtypes")
        PersonIncoming value = (PersonIncoming) obj;
        if (person != null ? !person.equals(value.person) : value.person != null) {
            return false;
        }
        return incoming == value.incoming;
    }
}
