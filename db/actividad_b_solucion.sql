-- ============================================================
-- ACTIVIDAD B: Control de Asistencia de Empleados - Solución
-- ============================================================

-- ------------------------------------------------------------
-- 1. FUNCIÓN: fn_horas_trabajadas
-- ------------------------------------------------------------
-- DECISIÓN TÉCNICA:
-- Se eligió una Función porque el objetivo es realizar un cálculo y RETORNAR un valor 
-- específico (NUMERIC). Las funciones pueden integrarse directamente dentro de sentencias 
-- SELECT, lo cual es ideal para cálculos matemáticos en línea.
-- 
-- DECISIÓN TÉCNICA:
-- - Se utilizó EXTRACT porque al restar dos campos TIMESTAMP el motor devuelve un tipo 
--   INTERVAL. EPOCH convierte este intervalo a segundos exactos, lo que permite dividir entre 3600.0 
--   para obtener horas decimales precisas.
-- - Se utilizó COALESCE para evitar que el sistema retorne un valor NULL si el empleado 
--   no tiene asistencias válidas en ese mes, forzando un 0 lógico.

CREATE OR REPLACE FUNCTION fn_horas_trabajadas(
    p_empleado_id INT,
    p_mes INT,
    p_anio INT
) 
RETURNS NUMERIC AS $$
DECLARE
    v_total_horas NUMERIC;
BEGIN
    SELECT COALESCE(SUM(EXTRACT(EPOCH FROM (salida - entrada)) / 3600.0), 0)
    INTO v_total_horas
    FROM registros_asistencia
    WHERE empleado_id = p_empleado_id
      AND EXTRACT(MONTH FROM fecha) = p_mes
      AND EXTRACT(YEAR FROM fecha) = p_anio
      AND entrada IS NOT NULL 
      AND salida IS NOT NULL;

    RETURN ROUND(v_total_horas, 2);
END;
$$ LANGUAGE plpgsql;


-- ------------------------------------------------------------
-- 2. TRIGGER: fn_trg_validar_entrada
-- ------------------------------------------------------------
-- DECISIÓN TÉCNICA:
-- Se utilizó BEFORE INSERT porque necesitamos interceptar y evaluar el dato ANTES de que 
-- se escriba en el disco. Si usáramos AFTER, el registro inválido ya estaría guardado y 
-- tendríamos que hacer un ROLLBACK completo o un DELETE, lo cual es más costoso en rendimiento.
--
-- DECISIÓN TÉCNICA:
-- Se utilizó EXISTS en lugar de COUNT(*) > 0 por eficiencia. EXISTS realiza una evaluación de 
-- cortocircuito (short-circuit): en cuanto encuentra la primera fila que cumple la condición 
-- de tener una entrada sin salida, detiene la búsqueda y lanza la excepción.

CREATE OR REPLACE FUNCTION fn_trg_validar_entrada()
RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (
        SELECT 1 
        FROM registros_asistencia 
        WHERE empleado_id = NEW.empleado_id 
          AND fecha = NEW.fecha 
          AND entrada IS NOT NULL 
          AND salida IS NULL
    ) THEN
        RAISE EXCEPTION 'El empleado % ya tiene una entrada abierta el día %. Registre la salida primero.', NEW.empleado_id, NEW.fecha;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_validar_entrada ON registros_asistencia;

CREATE TRIGGER trg_validar_entrada
BEFORE INSERT ON registros_asistencia
FOR EACH ROW
EXECUTE FUNCTION fn_trg_validar_entrada();


-- ------------------------------------------------------------
-- 3. STORED PROCEDURE: sp_cerrar_dia
-- ------------------------------------------------------------
-- DECISIÓN TÉCNICA:
-- Esta es la decisión más importante. Se requería hacer un COMMIT por cada registro procesado 
-- para no perder el progreso si un registro fallaba. Las Funciones en PostgreSQL se ejecutan 
-- dentro de un único bloque transaccional y NO permiten usar COMMIT o ROLLBACK en su interior. 
-- Los Stored Procedures sí permiten el control manual de transacciones.
--
-- DECISIÓN TÉCNICA:
-- Se iteró fila por fila usando un cursor implícito (FOR LOOP) porque cada fila requiere múltiples 
-- acciones independientes: actualizar el dato a las 18:00, sumar al contador, emitir el NOTICE 
-- personalizado y ejecutar el COMMIT.

CREATE OR REPLACE PROCEDURE sp_cerrar_dia(INOUT p_cerrados INT DEFAULT 0)
LANGUAGE plpgsql
AS $$
DECLARE
    v_registro RECORD;
BEGIN
    p_cerrados := 0; 

    FOR v_registro IN 
        SELECT r.id, e.nombre, r.fecha 
        FROM registros_asistencia r
        JOIN empleados e ON r.empleado_id = e.id 
        WHERE r.fecha = CURRENT_DATE - 1
          AND r.entrada IS NOT NULL
          AND r.salida IS NULL
    LOOP
        UPDATE registros_asistencia
        SET salida = v_registro.fecha + interval '18 hours'
        WHERE id = v_registro.id;

        p_cerrados := p_cerrados + 1;
        RAISE NOTICE 'Cierre automático aplicado: % en la fecha %', v_registro.nombre, v_registro.fecha;
        
        COMMIT;
    END LOOP;
END;
$$;


-- ------------------------------------------------------------
-- 4. VISTA: v_ranking_asistencia
-- ------------------------------------------------------------
-- DECISIÓN TÉCNICA:
-- Se creó una Vista para encapsular una consulta de lectura analítica compleja, permitiendo 
-- que se consulte fácilmente.
-- Se utilizó un CTE (WITH) para dividir el problema en dos pasos lógicos:
-- 1. Aislar el cálculo del promedio de horas por empleado (evitando que los cálculos de 
--    agregación se mezclen y generen errores de granularidad).
-- 2. Agrupar los resultados limpios del CTE a nivel de departamento. Esto mejora la 
--    legibilidad del código y facilita su mantenimiento.

CREATE OR REPLACE VIEW v_ranking_asistencia AS
WITH promedio_empleado AS (
    SELECT 
        e.departamento_id,
        e.id AS empleado_id,
        AVG(EXTRACT(EPOCH FROM (r.salida - r.entrada)) / 3600.0) AS promedio_horas_diarias
    FROM empleados e
    JOIN registros_asistencia r ON e.id = r.empleado_id
    WHERE r.entrada IS NOT NULL 
      AND r.salida IS NOT NULL
    GROUP BY e.departamento_id, e.id
)
SELECT 
    d.nombre AS departamento,
    COUNT(pe.empleado_id) AS cantidad_empleados,
    ROUND(AVG(pe.promedio_horas_diarias), 2) AS promedio_horas_departamento
FROM departamentos d
JOIN promedio_empleado pe ON d.id = pe.departamento_id
GROUP BY d.id, d.nombre
ORDER BY promedio_horas_departamento DESC;

