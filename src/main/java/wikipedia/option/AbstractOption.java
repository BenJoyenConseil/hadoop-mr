package wikipedia.option;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractOption<T> implements IOption{
    protected static Logger LOG = LoggerFactory.getLogger(AbstractOption.class);
    String code;
    T value;

    public AbstractOption(String code){
        this.code = code;
    }

    public AbstractOption(String code, String[] args){
        this.code = code;
        setValueFromArgs(args);
    }

    public void setValueFromArgs(String[] args){
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (!arg.equalsIgnoreCase(getCode()))
                continue;

            if (args.length - 1 < i + 1) {
                error();
                break;
            }

            setValueFromArg(args[i + 1]);
        }
    }

    public boolean contains(String[] args) {
        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            if (arg.equalsIgnoreCase(getCode()))
                return true;
        }
        return false;
    }

    protected abstract void setValueFromArg(String arg);

    protected void error(){
        LOG.warn("La valeur de l'option " + getCode() + " est manquante !");
    }

    public String getCode() {
        return code;
    }
    protected void setCode(String code) {
        this.code = code;
    }

    public T getValue() {
        return value;
    }
    public void setValue(T value) {
        this.value = value;
    }
}
