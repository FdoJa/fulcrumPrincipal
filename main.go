package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"

	pb "github.com/FdoJa/ServidoresFulcrum/proto"
	"google.golang.org/grpc"
)

// Variable asociada al identificador del servidor
var fulcrumId = 0 // Dado que un array empieza desde 0 se tiene el mapeo -> Fulcrum1 = 0 ; Fulcrum2 = 1 ; Fulcrum3 = 2
var timeVector = [3]int{0, 0, 0}

var confirmados = make(map[string]map[string]bool)
var listaLogs1 *pb.LogList
var listaLogs2 *pb.LogList

type Dato struct {
	Tiempo         time.Time
	Accion         string
	SectorAfectado string
	BaseAfectada   string
	NuevoValor     string
}

type fulcrumServer struct {
	pb.UnimplementedInformantesServer
}

func (s *fulcrumServer) AgregarBase(ctx context.Context, base *pb.Base) (*pb.Recepcion, error) {
	log.Printf("Comando recibido: AgregarBase\n")
	log.Printf("- Datos: %s %s %s\n", base.Sector, base.Base, base.Soldados)

	agregarDato(base.Sector, base.Base, base.Soldados, false)

	timeVector[fulcrumId]++

	return &pb.Recepcion{
		Ok: "OK",
	}, nil
}

func agregarDato(sector string, base string, soldados string, merge bool) {
	currentTime := time.Now()

	filePath := "/app/" + sector + ".txt"

	if merge {
		filePath = "/app/temp.txt"
	}

	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Fatalf("Error al leer archivo: %v", err)
	}
	defer file.Close()

	existia := false
	var text []string

	if merge {
		scanner := bufio.NewScanner(file)

		for scanner.Scan() {
			line := scanner.Text()
			data := strings.Fields(line)

			sectorTxt := data[0]
			baseTxt := data[1]
			soldadosTxt := data[2]

			if sectorTxt == sector {
				if baseTxt == base && soldadosTxt == soldados {
					return
				} else if baseTxt == base && soldadosTxt != soldados {
					data[2] = soldados
					existia = true
				}
			}

			newLine := strings.Join(data, " ")
			text = append(text, newLine)
		}

		if existia {
			file.Seek(0, 0)
			file.Truncate(0)

			for _, linea := range text {
				_, err := file.WriteString(linea + " \n")
				if err != nil {
					log.Fatalf("Error al modificar archivo: %v", err)
				}
			}
			return
		}
	}

	data := sector + " " + base + " " + soldados + " \n"
	_, err = file.WriteString(data)
	if err != nil {
		log.Fatalf("Error al escribir en el archivo: %v", err)
	}

	// Luego de realizar la correcta modificación, pasar a colocarla en los logs
	logLine := "AgregarBase " + sector + " " + base + " " + soldados + " \n"
	agregarLog(logLine, currentTime)
}

func (s *fulcrumServer) RenombrarBase(ctx context.Context, nuevaBase *pb.BaseModificada) (*pb.Recepcion, error) {
	log.Printf("Comando recibido: RenombrarBase\n")
	log.Printf("- Datos: %s %s %s\n", nuevaBase.Sector, nuevaBase.Base, nuevaBase.ActualizacionBase)

	modificarBase(nuevaBase.Sector, nuevaBase.Base, nuevaBase.ActualizacionBase, false)

	timeVector[fulcrumId]++

	return &pb.Recepcion{
		Ok: "OK",
	}, nil
}

func modificarBase(sector string, base string, nuevaBase string, merge bool) {
	currentTime := time.Now()

	filePath := "/app/" + sector + ".txt"

	if merge {
		filePath = "/app/temp.txt"
	}

	file, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		if merge {
			return
		}

		// En caso de que no existe el archivo, se crea
		file, err = os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			log.Fatalf("Error al abrir o crear el archivo: %v", err)
		}
		defer file.Close()

		data := sector + " " + nuevaBase + " 0 \n"
		_, err = file.WriteString(data)
		if err != nil {
			log.Fatalf("Error al escribir en el archivo: %v", err)
		}

		logLine := "RenombrarBase " + sector + " " + base + " " + nuevaBase + " \n"
		agregarLog(logLine, currentTime)
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var text []string

	for scanner.Scan() {
		line := scanner.Text()
		data := strings.Fields(line)

		sectorTxt := data[0]
		baseTxt := data[1]

		if merge {
			if sectorTxt == sector {
				if baseTxt == nuevaBase {
					continue
				}

				if baseTxt == base {
					data[1] = nuevaBase
				}
			}
		} else {
			if baseTxt == nuevaBase {
				continue
			}

			if baseTxt == base {
				data[1] = nuevaBase
			}
		}

		newLine := strings.Join(data, " ")
		text = append(text, newLine)
	}

	file.Seek(0, 0)
	file.Truncate(0)

	for _, linea := range text {
		_, err := file.WriteString(linea + " \n")
		if err != nil {
			log.Fatalf("Error al modificar archivo: %v", err)
		}
	}

	// Luego de realizar la correcta modificación, pasar a colocarla en los logs
	logLine := "RenombrarBase " + sector + " " + base + " " + nuevaBase + " \n"
	agregarLog(logLine, currentTime)
}

func (s *fulcrumServer) ActualizarValor(ctx context.Context, actualizar *pb.ActualizarSoldados) (*pb.Recepcion, error) {
	log.Printf("Comando recibido: ActualizarValor\n")
	log.Printf("- Datos: %s %s %s\n", actualizar.Sector, actualizar.Base, actualizar.ActualizacionSoldados)

	actualizarCantidad(actualizar.Sector, actualizar.Base, actualizar.ActualizacionSoldados, false)

	timeVector[fulcrumId]++

	return &pb.Recepcion{
		Ok: "OK",
	}, nil
}

func actualizarCantidad(sector string, base string, soldados string, merge bool) {
	currentTime := time.Now()

	filePath := "/app/" + sector + ".txt"

	if merge {
		filePath = "/app/temp.txt"
	}

	file, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		if merge {
			return
		}

		// En caso de que no existe el archivo, se crea
		file, err = os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			log.Fatalf("Error al abrir o crear el archivo: %v", err)
		}
		defer file.Close()

		data := sector + " " + base + " " + soldados + " \n"
		_, err = file.WriteString(data)
		if err != nil {
			log.Fatalf("Error al escribir en el archivo: %v", err)
		}

		logLine := "ActualizarValor " + sector + " " + base + " " + soldados + " \n"
		agregarLog(logLine, currentTime)
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var text []string

	for scanner.Scan() {
		line := scanner.Text()
		data := strings.Fields(line)

		sectorTxt := data[0]
		baseTxt := data[1]

		if merge {
			if sectorTxt == sector {
				if baseTxt == base {
					data[2] = soldados
				}

			}
		} else {
			if baseTxt == base {
				data[2] = soldados
			}
		}

		newLine := strings.Join(data, " ")
		text = append(text, newLine)
	}

	file.Seek(0, 0)
	file.Truncate(0)

	for _, linea := range text {
		_, err := file.WriteString(linea + " \n")
		if err != nil {
			log.Fatalf("Error al modificar archivo: %v", err)
		}
	}

	// Luego de realizar la correcta modificación, pasar a colocarla en los logs
	logLine := "ActualizarValor " + sector + " " + base + " " + soldados + " \n"
	agregarLog(logLine, currentTime)
}

func (s *fulcrumServer) BorrarBase(ctx context.Context, borrar *pb.Base) (*pb.Recepcion, error) {
	log.Printf("Comando recibido: BorrarBase\n")
	log.Printf("- Datos: %s %s\n", borrar.Base, borrar.Sector)

	borrarDato(borrar.Sector, borrar.Base, false)

	timeVector[fulcrumId]++

	return &pb.Recepcion{
		Ok: "OK",
	}, nil
}

func borrarDato(sector string, base string, merge bool) {
	currentTime := time.Now()

	filePathData := "/app/" + sector + ".txt"

	if merge {
		filePathData = "/app/temp.txt"
	}

	fileData, err := os.OpenFile(filePathData, os.O_RDWR, 0644)
	if err != nil {
		if merge {
			return
		}

		logLine := "BorrarBase " + sector + " " + base + " \n"
		agregarLog(logLine, currentTime)
		return
	}
	defer fileData.Close()

	scanner := bufio.NewScanner(fileData)
	var text []string

	for scanner.Scan() {
		line := scanner.Text()
		data := strings.Fields(line)

		sectorTxt := data[0]
		baseTxt := data[1]

		if merge {
			if sectorTxt == sector {
				if baseTxt == base {
					continue
				}
			}
		} else {
			if baseTxt == base {
				continue
			}
		}

		newLine := strings.Join(data, " ")
		text = append(text, newLine)
	}

	fileData.Seek(0, 0)
	fileData.Truncate(0)

	for _, linea := range text {
		_, err := fileData.WriteString(linea + " \n")
		if err != nil {
			log.Fatalf("Error al modificar archivo: %v", err)
		}
	}

	// Luego de realizar la correcta modificación, pasar a colocarla en los logs
	logLine := "BorrarBase " + sector + " " + base + " \n"
	agregarLog(logLine, currentTime)
}

func agregarLog(datos string, currentTime time.Time) {
	filePathLog := "/app/log.txt"

	fileLog, err := os.OpenFile(filePathLog, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Fatalf("Error en manejo de logs: %v", err)
	}
	defer fileLog.Close()

	logLine := currentTime.Format("15:04:05.000000") + " " + datos
	_, err = fileLog.WriteString(logLine)
	if err != nil {
		log.Fatalf("Error al escribir en el archivo de logs: %v", err)
	}
}

// Algoritmo para ordenar array (No esta permitido biblioteca Sort)
func bubbleSort(datos []Dato) {
	n := len(datos)
	for i := 0; i < n-1; i++ {
		for j := 0; j < n-i-1; j++ {
			if datos[j].Tiempo.After(datos[j+1].Tiempo) {
				// Intercambiar datos[j] y datos[j+1]
				datos[j], datos[j+1] = datos[j+1], datos[j]
			}
		}
	}
}

func hacerMerge(logData []Dato) (enviar *pb.Datos) {
	// Primero obtener los logs del servidor principal
	filePathLog := "/app/log.txt"

	fileLog, err := os.OpenFile(filePathLog, os.O_RDWR, 0644)
	if err != nil {
		log.Fatalf("Error en manejo de logs: %v", err)
	}
	defer fileLog.Close()

	scanner := bufio.NewScanner(fileLog)

	for scanner.Scan() {
		line := scanner.Text()
		data := strings.Fields(line)

		tiempo, err := time.Parse("15:04:05.000000", data[0])
		if err != nil {
			fmt.Println("Error al parsear el tiempo:", err)
		}

		dato := Dato{
			Tiempo:         tiempo,
			Accion:         data[1],
			SectorAfectado: data[2],
			BaseAfectada:   data[3],
			NuevoValor:     data[4],
		}

		logData = append(logData, dato)
	}

	bubbleSort(logData)

	bases := make(map[string]map[string][]Dato)

	log.Printf("Datos totales:")
	for _, dato := range logData {
		log.Printf(dato.Tiempo.String() + " " + dato.Accion + " " + dato.SectorAfectado + " " + dato.BaseAfectada + " " + dato.NuevoValor)

		if bases[dato.SectorAfectado] == nil {
			bases[dato.SectorAfectado] = make(map[string][]Dato)
		}

		bases[dato.SectorAfectado][dato.BaseAfectada] = append(bases[dato.SectorAfectado][dato.BaseAfectada], dato)
	}

	revisados := make(map[string]map[string]bool)

	for _, dato := range logData {
		sectorActual := dato.SectorAfectado
		baseActual := dato.BaseAfectada

		_, ok := revisados[sectorActual][baseActual]
		if ok {
			continue
		}

		datosActuales := bases[sectorActual][baseActual]

		log.Printf("------------- Separador -------------- ")
		for _, dato := range datosActuales {
			log.Printf("Datos para el sector %s y la base %s: %s %s %s %s %s",
				sectorActual, baseActual, dato.Tiempo.String(), dato.Accion, dato.SectorAfectado, dato.BaseAfectada, dato.NuevoValor)

			switch dato.Accion {
			case "AgregarBase":
				log.Printf("-- Agregando dato: %s %s %s %s %s", dato.Tiempo.String(), dato.Accion, dato.SectorAfectado, dato.BaseAfectada, dato.NuevoValor)

				agregarDato(dato.SectorAfectado, dato.BaseAfectada, dato.NuevoValor, true)

				if confirmados[sectorActual] == nil {
					confirmados[sectorActual] = make(map[string]bool)
				}

				confirmados[sectorActual][baseActual] = true

			case "RenombrarBase":
				_, ok := confirmados[sectorActual][baseActual]
				if ok {
					log.Printf("-- Modificando datos: %s %s %s %s %s", dato.Tiempo.String(), dato.Accion, dato.SectorAfectado, dato.BaseAfectada, dato.NuevoValor)
					modificarBase(dato.SectorAfectado, dato.BaseAfectada, dato.NuevoValor, true)
					confirmados[sectorActual][dato.NuevoValor] = true
					confirmados[sectorActual][dato.SectorAfectado] = false

					tiempoActual := dato.Tiempo

					log.Printf("---- Intentando entrar a la caga cuando se modifica el nombre: ")
					for _, datoCambiado := range bases[dato.SectorAfectado][dato.NuevoValor] {
						if datoCambiado.Tiempo.After(tiempoActual) {
							log.Printf("------ Modificando datos: %s %s %s %s %s", datoCambiado.Tiempo.String(), datoCambiado.Accion, datoCambiado.SectorAfectado, datoCambiado.BaseAfectada, datoCambiado.NuevoValor)
							switch datoCambiado.Accion {
							case "AgregarBase":
								agregarDato(datoCambiado.SectorAfectado, datoCambiado.BaseAfectada, datoCambiado.NuevoValor, true)

								if confirmados[sectorActual] == nil {
									confirmados[sectorActual] = make(map[string]bool)
								}

								confirmados[sectorActual][datoCambiado.BaseAfectada] = true
							case "RenombrarBase":
								_, ok := confirmados[sectorActual][datoCambiado.BaseAfectada]

								if ok {
									log.Printf("---- Modificando datos: %s %s %s %s %s", datoCambiado.Tiempo.String(), datoCambiado.Accion, datoCambiado.SectorAfectado, datoCambiado.BaseAfectada, datoCambiado.NuevoValor)
									modificarBase(dato.SectorAfectado, dato.BaseAfectada, dato.NuevoValor, true)

								}
							case "ActualizarValor":
								_, ok := confirmados[sectorActual][datoCambiado.BaseAfectada]

								if ok {
									log.Printf("---- Modificando datos: %s %s %s %s %s", datoCambiado.Tiempo.String(), datoCambiado.Accion, datoCambiado.SectorAfectado, datoCambiado.BaseAfectada, datoCambiado.NuevoValor)
									actualizarCantidad(datoCambiado.SectorAfectado, datoCambiado.BaseAfectada, datoCambiado.NuevoValor, true)
								} else {
									borrarDato(datoCambiado.SectorAfectado, datoCambiado.BaseAfectada, true)
								}

							case "BorrarBase":
								_, ok := confirmados[sectorActual][datoCambiado.BaseAfectada]

								if ok {
									log.Printf("---- Borrando datos: %s %s %s %s %s", datoCambiado.Tiempo.String(), datoCambiado.Accion, datoCambiado.SectorAfectado, datoCambiado.BaseAfectada, datoCambiado.NuevoValor)
									borrarDato(datoCambiado.SectorAfectado, datoCambiado.BaseAfectada, true)
								} else {
									log.Printf("---- Dato no existe, no borrar nada y pasar al siguiente")
								}
							}
						}
					}

					if revisados[sectorActual] == nil {
						revisados[sectorActual] = make(map[string]bool)
					}

					revisados[sectorActual][dato.NuevoValor] = true

				} else {
					log.Printf("-- Dato aún no existia así que no modificar base, borrando de ser necesario")
					borrarDato(dato.SectorAfectado, dato.BaseAfectada, true)
				}

			case "ActualizarValor":
				_, ok := confirmados[sectorActual][baseActual]

				if ok {
					log.Printf("-- Modificando datos: %s %s %s %s %s", dato.Tiempo.String(), dato.Accion, dato.SectorAfectado, dato.BaseAfectada, dato.NuevoValor)
					actualizarCantidad(dato.SectorAfectado, dato.BaseAfectada, dato.NuevoValor, true)
				} else {
					log.Printf("-- Dato aún no existia así que no modificar soldados, borrando de ser necesario")
					borrarDato(dato.SectorAfectado, dato.BaseAfectada, true)
				}
			case "BorrarBase":
				_, ok := confirmados[sectorActual][baseActual]

				if ok {
					log.Printf("-- Borrando datos: %s %s %s %s %s", dato.Tiempo.String(), dato.Accion, dato.SectorAfectado, dato.BaseAfectada, dato.NuevoValor)
					borrarDato(dato.SectorAfectado, dato.BaseAfectada, true)
					confirmados[sectorActual][baseActual] = false
				} else {
					log.Printf("-- Dato no existe, no borrar nada y pasar al siguiente")
				}
			}
		}

		if revisados[sectorActual] == nil {
			revisados[sectorActual] = make(map[string]bool)
		}

		revisados[sectorActual][baseActual] = true
	}

	var listaBases []*pb.Base

	filePathTemp := "/app/temp.txt"
	fileTemp, err := os.OpenFile(filePathTemp, os.O_RDWR, 0644)
	if err != nil {
		fmt.Printf("Error al abrir o crear el archivo temporal: %v\n", err)
	}
	defer fileTemp.Close()

	log.Printf(".....Enviando los siguientes datos a los otros servidores")

	scannerData := bufio.NewScanner(fileTemp)
	sectoresRevisados := make(map[string]bool)

	for scannerData.Scan() {
		line := scannerData.Text()
		data := strings.Fields(line)

		sectorTxt := data[0]

		baseEnviar := &pb.Base{
			Sector:   data[0],
			Base:     data[1],
			Soldados: data[2],
		}

		listaBases = append(listaBases, baseEnviar)

		sectorFilePath := fmt.Sprintf("/app/" + data[0] + ".txt")
		sectorFile, err := os.OpenFile(sectorFilePath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
		if err != nil {
			log.Fatalf("Error al abrir o crear el archivo %s: %v", sectorFilePath, err)
		}
		defer sectorFile.Close()

		if !sectoresRevisados[sectorTxt] {
			sectoresRevisados[sectorTxt] = true
			sectorFile.Seek(0, 0)
			sectorFile.Truncate(0)
		}

		_, err = sectorFile.WriteString(fmt.Sprintf("%s\n", line))
		if err != nil {
			log.Fatalf("Error al escribir en %s: %v", sectorFilePath, err)
		}
	}

	fileLogPath := "/app/log.txt"
	errLog := os.Remove(fileLogPath)
	if err != nil {
		log.Fatalf("Error borrar logs: %v", errLog)
	}

	response := &pb.Datos{
		ListaBases: listaBases,
	}

	return response
}

func main() {
	lis, err := net.Listen("tcp", ":50052")
	if err != nil {
		log.Fatalf("Fallo en escuchar: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterInformantesServer(s, &fulcrumServer{})

	go func() {
		fmt.Println("Servidor Fulcrum1 (principal) escuchando")
		if err := s.Serve(lis); err != nil {
			log.Fatalf("Fallo en serve: %v", err)
		}
	}()

	timer := time.NewTicker(1 * time.Minute)

	fulcrum2Addr := "fulcrum2-container:50053"
	//fulcrum3Addr := "fulcrum3-container:50054"

	for {
		select {
		case <-timer.C:
			log.Printf("Ejecutando merge...\n")
			var logData []Dato

			// Servidor Fulcrum2
			conn1, err := grpc.Dial(fulcrum2Addr, grpc.WithInsecure())
			if err != nil {
				log.Fatalf("No se pudo conectar al servidor Fulcrum 2: %v", err)
			}

			client1 := pb.NewConsistenciaClient(conn1)

			res, err := client1.ConseguirLogs(context.Background(), &pb.Recepcion{
				Ok: "Ok",
			})

			if err != nil {
				log.Fatalf("Error al pedir Logs de servidor Fulcrum 2: %v", err)
			} else {
				log.Printf("-- Llegando datos:\n")

				for _, linea := range res.ListaLogs {
					log.Printf("---> Tiempo: %s, Accion: %s, Sector Afectado: %s, Base Afectada: %s, Nuevo Valor: %s\n",
						linea.Tiempo, linea.Accion, linea.SectorAfectado, linea.BaseAfectada, linea.NuevoValor)

					tiempo, err := time.Parse("15:04:05.000000", linea.Tiempo)
					if err != nil {
						log.Fatalf("Error al parsear el tiempo: %v", err)
					}

					dato := Dato{
						Tiempo:         tiempo,
						Accion:         linea.Accion,
						SectorAfectado: linea.SectorAfectado,
						BaseAfectada:   linea.BaseAfectada,
						NuevoValor:     linea.NuevoValor,
					}

					logData = append(logData, dato)
				}
			}

			enviar := hacerMerge(logData)

			resTxt, err := client1.EnviarDatosActualizados(context.Background(), &pb.Datos{
				ListaBases: enviar.ListaBases,
			})

			if err != nil {
				log.Fatalf("Error al hacer consistencia: %v", err)
			} else {
				log.Printf(resTxt.Ok)
			}

			conn1.Close()
			/*
				//	Servidor Fulcrum3
				conn2, err := grpc.Dial(fulcrum3Addr, grpc.WithInsecure())
				if err != nil {
					log.Fatalf("No se pudo conectar al servidor Fulcrum 3: %v", err)
				}
				defer conn2.Close()

				client2 := pb.NewConsistenciaClient(conn2)

				res2, err := client2.ConseguirLogs(context.Background(), &pb.Recepcion{
					Ok: "Ok",
				})

				if err != nil {
					log.Fatalf("Error al pedir Logs de servidor Fulcrum 3: %v", err)
				} else {
					for _, linea := range res2.ListaLogs {
						// Convertir el tiempo a time.Time
						tiempo, err := time.Parse("15:04:05.000000", linea.Tiempo)
						if err != nil {
							fmt.Println("Error al parsear el tiempo:", err)
						}

						dato := Dato{
							Tiempo:         tiempo,
							Accion:         linea.Accion,
							SectorAfectado: linea.SectorAfectado,
							BaseAfectada:   linea.BaseAfectada,
							NuevoValor:     linea.NuevoValor,
							Servidor: "Servidor3"
						}

						logData = append(logData, dato)
					}
				}
			*/
		}
	}
}
