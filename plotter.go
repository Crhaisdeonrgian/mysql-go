package sql

import (
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"

	"log"
	"os"
)

const (
	IgorFilePath string = "/Users/igorvozhga/DIPLOMA/"
	MikeFilePath string = "/home/user/go/"
)

type xy struct{ x, y float64 }

func makePlot(xys plotter.XYs) {
	f, err := os.Create(IgorFilePath + "plot.png")
	if err != nil {
		log.Fatal("cannot open file ", err)
	}
	p := plot.New()

	s, err := plotter.NewScatter(xys)
	if err != nil {
		log.Fatal("cannot create scatter ", err)
	}
	p.Add(s)
	wt, err := p.WriterTo(256, 256, "png")
	if err != nil {
		log.Fatal("cannot create writer ", err)
	}
	_, err = wt.WriteTo(f)
	if err != nil {
		log.Fatal("cannot write to file ", err)
	}
	err = f.Close()
	if err != nil {
		log.Fatal("cannot close file ", err)
	}
}
