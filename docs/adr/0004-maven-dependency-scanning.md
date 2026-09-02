# ADR 0004 — Filesystem Trivy for the Maven worker (not OWASP dependency-check)

## Contexto

El worker Java son ~3 archivos / ~170 líneas. Eso no es la superficie de
supply chain: `mvn dependency:tree` resuelve ~85 artefactos, ~52 en
scope compile/runtime (AWS SDK v2 arrastra ~25 módulos, Netty ~11, más
Spring Framework 7 / Tomcat embed / Jackson). Escanear "porque el código
propio es chico" era el criterio equivocado.

`.github/dependabot.yml` ya cubre Maven (`directory: /src/worker`,
semanal). Dependabot alerta transitivas, no solo directas. Eso no
reemplaza un gate en **push y schedule**: Dependabot es asíncrono; el
job de CI falla el build el mismo commit que introduce un CVE HIGH/CRITICAL.

## Decisión

Añadir **Trivy en modo filesystem** (`trivy fs src/worker`) al job
`security` ya aislado de `test`/`e2e`.

- Base de vulnerabilidades compacta distribuida por OCI (decenas de MB,
  cacheable). No descarga el feed NVD completo. No exige API key.
- Corre en push (y puede correr en schedule si se añade al `on:`), no
  solo en el diff de un PR.
- Un fallo de scan **no** bloquea el job `test` ni el demo.

## Alternativas consideradas

- **dependency-check-maven + caché NVD**: es el scanner "Maven-nativo".
  Exige API key de NVD y gestión de caché del feed — exactamente la
  complejidad que se evitó en la ronda anterior.
- **Snyk**: cuenta + `SNYK_TOKEN`. Fuera de alcance para un repo de
  portafolio.
- **dependency-review-action**: gratis y cero-config, pero solo diffs
  de PR. Un push directo a `main` (o un schedule) no lo ve.

## Qué no duplica Dependabot

| | Dependabot | Job Trivy |
|---|---|---|
| Cuándo | alerta semanal / PR de bump | cada push del job `security` |
| Qué | PRs para bump; alertas de transitives | gate HIGH/CRITICAL en el árbol actual |
| Acción | humano mergea el bump | CI rojo hasta que se actualice o se documente el CVE |

Si Trivy reporta un CVE real: actualizar la dependencia, o documentar
la excepción en este ADR. **No** bajar la severidad del gate para
hacer pasar el build.

El scan necesita el árbol Maven ya resuelto en `~/.m2`. Sin eso Trivy
pega a Maven Central por cada POM transitivo y el IP acaba en HTTP 429.
En CI: `setup-java` (cache maven) + `./mvnw dependency:resolve` antes
del paso Trivy. En local: `mvn -q package` (o `dependency:resolve`) y
montar `~/.m2` si se corre Trivy en Docker.

## Consecuencias

`pip-audit` sigue cubriendo Python. Trivy cubre el árbol Maven de
`src/worker`. El job `security` sigue aislado: un CVE nuevo no tumba
`make demo`.
