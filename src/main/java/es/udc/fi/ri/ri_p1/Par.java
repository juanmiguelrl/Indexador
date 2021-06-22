package es.udc.fi.ri.ri_p1;

public class Par {
    private String term;
    private Double sim;

	public Par(String term, Double sim) {
        this.term = term;
        this.sim = sim;
    }

    public String getTerm() {
        return term;
    }

    public void setTerm(String term) {
        this.term = term;
    }

    public Double getSim() {
        return sim;
    }

    public void setSim(Double sim) {
        this.sim = sim;
    }
}
