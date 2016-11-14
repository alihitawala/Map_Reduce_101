/**
 * Created by aliHitawala on 10/4/16.
 */
public class State {
    int x, y;

    public State(int x, int y) {
        this.x = x;
        this.y = y;
    }

    public int getX() {
        return x;
    }

    public void setX(int x) {
        this.x = x;
    }

    public int getY() {
        return y;
    }

    public void setY(int y) {
        this.y = y;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof State)) return false;

        State state = (State) o;

        if (getX() != state.getX()) return false;
        return getY() == state.getY();

    }

    @Override
    public int hashCode() {
        int result = getX();
        result = 31 * result + getY();
        return result;
    }

    public static void main(String[] args) {
        State s = new State(0,0);
        State b = new State(0,0);
        if (s.equals(b)) {
            System.out.println("Same");
        }
    }
}
