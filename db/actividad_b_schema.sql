-- ============================================================
-- ACTIVIDAD B: Control de Asistencia de Empleados
-- Base de Datos Avanzadas · UP Chiapas
-- Mtro. Ramsés Alejandro Camas Nájera
-- ============================================================
-- Ejecutar con: psql -f actividad_b_schema.sql
-- ============================================================

DROP TABLE IF EXISTS registros_asistencia CASCADE;
DROP TABLE IF EXISTS empleados CASCADE;
DROP TABLE IF EXISTS departamentos CASCADE;

-- ==================== TABLAS ====================

CREATE TABLE departamentos (
    id SERIAL PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL UNIQUE,
    ubicacion VARCHAR(100)
);

CREATE TABLE empleados (
    id SERIAL PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL,
    email VARCHAR(150) UNIQUE NOT NULL,
    departamento_id INT NOT NULL REFERENCES departamentos(id),
    puesto VARCHAR(100),
    activo BOOLEAN DEFAULT true,
    fecha_ingreso DATE DEFAULT CURRENT_DATE
);

CREATE TABLE registros_asistencia (
    id SERIAL PRIMARY KEY,
    empleado_id INT NOT NULL REFERENCES empleados(id),
    fecha DATE NOT NULL DEFAULT CURRENT_DATE,
    entrada TIMESTAMP,
    salida TIMESTAMP,
    CONSTRAINT chk_salida_despues CHECK (salida IS NULL OR salida > entrada)
);

-- ==================== DATOS DE PRUEBA ====================

-- Departamentos
INSERT INTO departamentos (nombre, ubicacion) VALUES
('Ingenieria', 'Edificio A, Piso 3'),
('Recursos Humanos', 'Edificio B, Piso 1'),
('Ventas', 'Edificio A, Piso 1'),
('Soporte Tecnico', 'Edificio C, Piso 2');

-- Empleados (16 empleados en 4 departamentos)
INSERT INTO empleados (nombre, email, departamento_id, puesto) VALUES
-- Ingeniería (5)
('Roberto Flores Méndez', 'roberto.flores@empresa.com', 1, 'Desarrollador Senior'),
('Sofía Reyes Castañeda', 'sofia.reyes@empresa.com', 1, 'Desarrolladora Full-Stack'),
('Andrés Vargas Ponce', 'andres.vargas@empresa.com', 1, 'DevOps Engineer'),
('Mariana Delgado Ríos', 'mariana.delgado@empresa.com', 1, 'QA Lead'),
('Pablo Guerrero Cruz', 'pablo.guerrero@empresa.com', 1, 'Desarrollador Junior'),
-- RRHH (3)
('Lucía Romero Aguilar', 'lucia.romero@empresa.com', 2, 'Coordinadora RRHH'),
('Tomás Ibáñez Solano', 'tomas.ibanez@empresa.com', 2, 'Reclutador'),
('Carmen Ortiz Vidal', 'carmen.ortiz@empresa.com', 2, 'Analista de Nómina'),
-- Ventas (4)
('Ricardo Peña Lara', 'ricardo.pena@empresa.com', 3, 'Gerente de Ventas'),
('Valeria Campos Herrera', 'valeria.campos@empresa.com', 3, 'Ejecutiva de Cuenta'),
('Óscar Mora Figueroa', 'oscar.mora@empresa.com', 3, 'Ejecutivo de Cuenta'),
('Natalia Silva Paredes', 'natalia.silva@empresa.com', 3, 'Asistente Comercial'),
-- Soporte (4)
('Diego Luna Estrada', 'diego.luna@empresa.com', 4, 'Líder de Soporte'),
('Paola Ríos Navarro', 'paola.rios@empresa.com', 4, 'Técnica de Soporte'),
('Héctor Zamora Bravo', 'hector.zamora@empresa.com', 4, 'Técnico de Soporte'),
('Isabel Medina Torres', 'isabel.medina@empresa.com', 4, 'Técnica de Soporte');

-- Registros de asistencia completos (últimas 2 semanas)
-- Generamos registros para varios días
DO $$
DECLARE
    d DATE;
    emp RECORD;
    h_entrada INT;
    h_salida INT;
BEGIN
    FOR d IN SELECT generate_series(CURRENT_DATE - 14, CURRENT_DATE - 2, '1 day'::interval)::date
    LOOP
        -- Saltar fines de semana
        IF EXTRACT(DOW FROM d) IN (0, 6) THEN CONTINUE; END IF;

        FOR emp IN SELECT id FROM empleados WHERE activo = true
        LOOP
            -- 90% de probabilidad de asistir
            IF random() < 0.9 THEN
                h_entrada := 7 + floor(random() * 2)::int;  -- 7:00 a 8:59
                h_salida := 16 + floor(random() * 3)::int;  -- 16:00 a 18:59
                INSERT INTO registros_asistencia (empleado_id, fecha, entrada, salida)
                VALUES (
                    emp.id,
                    d,
                    d + (h_entrada || ':' || (floor(random()*59)::int) || ':00')::interval,
                    d + (h_salida || ':' || (floor(random()*59)::int) || ':00')::interval
                );
            END IF;
        END LOOP;
    END LOOP;
END $$;

-- Registros de AYER sin salida (para probar sp_cerrar_dia)
INSERT INTO registros_asistencia (empleado_id, fecha, entrada, salida) VALUES
(1, CURRENT_DATE - 1, (CURRENT_DATE - 1) + interval '8 hours 15 minutes', NULL),
(5, CURRENT_DATE - 1, (CURRENT_DATE - 1) + interval '7 hours 45 minutes', NULL),
(13, CURRENT_DATE - 1, (CURRENT_DATE - 1) + interval '8 hours 30 minutes', NULL);

-- Registros de HOY (para probar trigger de entrada duplicada)
INSERT INTO registros_asistencia (empleado_id, fecha, entrada, salida) VALUES
(2, CURRENT_DATE, CURRENT_DATE + interval '8 hours 0 minutes', NULL);
-- ^ Sofía tiene entrada HOY sin salida → el trigger debe impedir otra entrada hoy

-- ==================== VERIFICACIÓN ====================
DO $$
BEGIN
    RAISE NOTICE '=== Schema Actividad B cargado correctamente ===';
    RAISE NOTICE 'Departamentos: %', (SELECT COUNT(*) FROM departamentos);
    RAISE NOTICE 'Empleados: %', (SELECT COUNT(*) FROM empleados);
    RAISE NOTICE 'Registros de asistencia: %', (SELECT COUNT(*) FROM registros_asistencia);
    RAISE NOTICE 'Registros sin salida (ayer): %', (SELECT COUNT(*) FROM registros_asistencia WHERE fecha = CURRENT_DATE - 1 AND salida IS NULL);
    RAISE NOTICE 'Registros sin salida (hoy): %', (SELECT COUNT(*) FROM registros_asistencia WHERE fecha = CURRENT_DATE AND salida IS NULL);
END $$;
