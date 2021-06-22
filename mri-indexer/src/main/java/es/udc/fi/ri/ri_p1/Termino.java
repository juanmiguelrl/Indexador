package es.udc.fi.ri.ri_p1;

public class Termino {
    private String name;
    private Double tf;
    private Double df;
    private Double tfxidf;

    public Termino(String name, Double tf, Double df, Double tfxidf) {
        this.name = name;
		this.tf = tf;
		this.df = df;
		this.tfxidf = tfxidf;
    }
    
    @Override
    public String toString() {
        return "term='" + name + '\'' +
                ", tf=" + tf.intValue() +
                ", df=" + df.intValue() +
                ", tfxidf=" + String.format("%.2f", tfxidf);
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setTf(Double tf) {
        this.tf = tf;
    }

    public void setDf(Double df) {
        this.df = df;
    }

    public void setTfxidf(Double tfxidf) {
        this.tfxidf = tfxidf;
    }

    public String getName() {
        return name;
    }

    public Double getTf() {
        return tf;
    }

    public Double getDf() {
        return df;
    }

    public Double getTfxidf() {
        return tfxidf;
    }
}
