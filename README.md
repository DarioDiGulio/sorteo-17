# Sorteo Club 17 de Octubre

App Flask + Postgres para la venta de números del sorteo.

```bash
docker compose up -d --build   # http://localhost:3001  —  admin: /admin/login
```

## Expansión automática de números

Los números a la venta se habilitan por bloques. Cuando el bloque actual está por agotarse, se agrega uno nuevo sin intervención manual.

| Parámetro | Dónde se define | Valor por defecto |
|---|---|---|
| Bloque inicial | Admin → Venta (`venta.bloqueInicial`) | 200 |
| Incremento | Admin → Venta (`venta.incremento`) | 50 |
| Umbral de expansión | `UMBRAL_EXPANSION` en `app.py` | 10 |

**Regla:** cuando quedan **10 números libres o menos** dentro del rango visible, el máximo aumenta en un incremento.

Ejemplo con los valores por defecto:

- 1 a 200 visibles. Con 189 vendidos (quedan 11) no cambia nada.
- Con 190 vendidos (quedan 10) se habilitan 1 a 250.
- Con 240 vendidos se habilitan 1 a 300, y así sucesivamente.

**Cuándo se evalúa:** cada vez que alguien carga la página pública `/numeros`, incluida la redirección que sigue a cada reserva. Por eso, en el uso normal, la expansión ocurre al instante. El panel de admin muestra el máximo guardado y no dispara la expansión por sí mismo.

**Detalles:**

- Cada carga de la página agrega como máximo un bloque.
- Liberar o borrar números no achica el rango. Para volver al bloque inicial, usá *Resetear máximo* en el admin. *Limpiar compradores* también lo reinicia.
- Para cambiar el umbral, modificá `UMBRAL_EXPANSION` en `app.py`.
