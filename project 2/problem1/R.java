package problem1;

import java.util.Random;

/**
 * Created by Yihao on 2017/2/21.
 */
public class R {

    static int id = 0;
    float x;
    float y;
    float h;
    float w;
    float min = 1.0f;
    float max = 10000.0f;
    float minh = 1.0f;
    float maxh = 20.0f;

    public R() {
        this.id ++;
        this.h = this.getH();
        this.w = this.getW();
        this.x = this.getX();
        this.y = this.getY();

    }

    private float getX() {
        Random rand = new Random();
        float result = rand.nextFloat() * (max - min) + min;
        while ((result + this.w) > 10000) {
            result = rand.nextFloat() * (max - min) + min;
        }
        return result;
    }

    private float getY() {
        Random rand = new Random();
        float result = rand.nextFloat() * (max - min) + min;
        while ((result + this.h) > 10000) {
            result = rand.nextFloat() * (max - min) + min;
        }
        return result;
    }

    private float getH() {
        Random rand = new Random();
        return rand.nextFloat() * (maxh - minh) + minh;
    }

    private float getW() {
        Random rand = new Random();
        return rand.nextFloat() * (maxh - minh) + minh;
    }
}
