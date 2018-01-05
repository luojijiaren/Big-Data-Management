package problem1; /**
 * Created by Yihao on 2017/2/21.
 */
import java.util.Random;

public class P {
    static int id = 0;
    float x;
    float y;
    float min = 1.0f;
    float max = 10000.0f;

    public P() {
        this.id ++;
        this.x = this.getX();
        this.y = this.getY();
    }

    private float getX() {
        Random rand = new Random();
        float result = rand.nextFloat() * (max - min) + min;
        return result;
    }

    private float getY() {
        Random rand = new Random();
        float result = rand.nextFloat() * (max - min) + min;
        return result;
    }

}
