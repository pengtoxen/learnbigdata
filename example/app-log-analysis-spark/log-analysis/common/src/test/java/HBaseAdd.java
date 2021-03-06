import com.peng.model.UserCityStatModel;
import com.peng.service.BehaviorStatService;
import com.peng.utils.PropertiesUtil;
import com.peng.utils.StringUtil;

import java.io.IOException;
import java.util.Properties;

public class HBaseAdd {

    public static void main(String[] args) {

        Properties prop = null;
        try {
            prop = PropertiesUtil.getProperties("data-processing/src/main/resources/config.properties");
        } catch (IOException e) {
            e.printStackTrace();
        }

        BehaviorStatService service = BehaviorStatService.getInstance(prop);

        UserCityStatModel model = new UserCityStatModel();
        model.setCity("Xinjiang");
        model.setUserId(StringUtil.getFixedLengthStr("10", 10));

        for (int i = 0; i <3 ; i++){
            service.addUserNumOfCity(model);
        }
    }
}
