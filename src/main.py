from src.app_logger import AppLogger
from src.simulation import LibrarySimulation
import src.constants as cst

def main() -> None:
    """
    Обязательнная составляющая программ, которые сдаются. Является точкой входа в приложение
    :return: Данная функция ничего не возвращает
    """
    step = int(input("Введите кол-во шагов симуляции: "))
    seed = input("Введите сид(или оставьте пустым): ")
    if seed:
        seed = int(seed)
    else:
        seed = None
    simulation = LibrarySimulation()
    AppLogger.configure_logger()
    print(simulation.run_simulation(step, seed))


if __name__ == "__main__":
    main()
