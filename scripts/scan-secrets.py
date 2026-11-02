#!/usr/bin/env python3
"""Repère les littéraux sensibles sans jamais restituer leur valeur."""

import argparse
import base64
import binascii
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys


JWT = re.compile(
    r"(?<![A-Za-z0-9_-])eyJ[A-Za-z0-9_-]*"
    r"\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+(?![A-Za-z0-9_-])"
)
FOURNISSEUR = re.compile(
    r"(?<![A-Za-z0-9_-])(?:sk-|gsk_)[A-Za-z0-9_-]+(?![A-Za-z0-9_-])"
    r"|(?<![A-Za-z0-9_])[A-Fa-f0-9]{32}(?![A-Za-z0-9_])"
)
AFFECTATION = re.compile(
    r'''(?<![\w$'"])(?P<nom>[A-Za-z_$][A-Za-z0-9_$]*)\s*'''
    r'(?::\s*[A-Za-z_$][A-Za-z0-9_$]*(?:\[\])?\s*)?'
    r'''(?:=(?!=|>)|:)\s*(?P<quote>['"])'''
    r'''(?P<valeur>(?:\\[^\r\n]|(?!(?P=quote))[^\\\r\n])*)(?P=quote)'''
)


def role_jwt(valeur):
    """Un payload absent ou malformé ne doit pas interrompre le scan."""
    try:
        payload = valeur.split('.')[1]
        contenu = base64.b64decode(
            payload + '=' * (-len(payload) % 4), altchars=b'-_', validate=True
        )
        role = json.loads(contenu.decode('utf-8')).get('role')
        # Empêcher un champ role hostile de restituer une valeur sensible.
        if (
            isinstance(role, str)
            and re.fullmatch(r'[A-Za-z_][A-Za-z0-9_:-]{0,63}', role)
            and not FOURNISSEUR.search(role)
        ):
            return role
    except (ValueError, UnicodeError, binascii.Error, AttributeError, RecursionError):
        pass
    return 'inconnu'


def trouvailles(texte):
    """Une sortie par occurrence, y compris les affectations sensibles."""
    jetons = list(JWT.finditer(texte))
    cles = [
        cle for cle in FOURNISSEUR.finditer(texte)
        if not any(cle.start() < jwt.end() and jwt.start() < cle.end() for jwt in jetons)
    ]
    occurrences = [
        (match.start(), match.end(), match.group(),
         role_jwt(match.group()) if match.re is JWT else 'non-JWT')
        for match in jetons + cles
    ]
    for match in AFFECTATION.finditer(texte):
        if not any(mot in match['nom'].upper() for mot in ('KEY', 'TOKEN', 'SECRET', 'PASSWORD')):
            continue
        valeur = match['valeur']
        if len(valeur) < 16 or valeur.lower().startswith('http'):
            continue
        debut, fin = match.span('valeur')
        if any(debut < autre_fin and autre_debut < fin for autre_debut, autre_fin, _, _ in occurrences):
            continue
        occurrences.append((debut, fin, valeur, 'non-JWT'))

    for debut, _, valeur, role in sorted(occurrences):
        numero = texte.count('\n', 0, debut) + 1
        empreinte = hashlib.sha256(valeur.encode('utf-8')).hexdigest()[:12]
        yield numero, role, empreinte


def test_interne():
    """Fixtures factices construites en mémoire, sans afficher les valeurs."""
    valeur = 'Ab3x' * 8
    empreinte = hashlib.sha256(valeur.encode('utf-8')).hexdigest()[:12]
    attendu = [(1, 'non-JWT', empreinte)]
    cas = []
    for nom in ('MISTRAL_API_KEY', 'accessToken', 'client_secret', 'Password'):
        for quote in ("'", '"'):
            cas.append((f'const {nom} = {quote}{valeur}{quote};', attendu))
    cas.extend([
        (f'const apiKey: string = "{valeur}";', attendu),
        (f'{{ apiKey: "{valeur}" }}', attendu),
        (f'const apiKey =\n  "{valeur}";', [(2, 'non-JWT', empreinte)]),
        ('apiKey = "x" * 32', []),
        ('apiKey = ""', []),
        ('apiKey = "' + 'x' * 15 + '"', []),
        (f'other = "{valeur}"', []),
        (f'apiKey == "{valeur}"', []),
        (f'apiKey => "{valeur}"', []),
        (f'apiKey = "https://example.invalid/{valeur}"', []),
        (f'apiKey = "HTTP://example.invalid/{valeur}"', []),
        (f'apiKey = Deno.env.get("{valeur}")', []),
        (f'apiKey = process.env["{valeur}"]', []),
        (f'apiKey = import.meta.env["{valeur}"]', []),
    ])
    for fournisseur in ('sk-' + valeur, 'gsk_' + valeur, 'a1' * 16):
        cas.append((f'apiKey = "{fournisseur}"', [
            (1, 'non-JWT', hashlib.sha256(fournisseur.encode('utf-8')).hexdigest()[:12])
        ]))
    for numero, (texte, resultat) in enumerate(cas, 1):
        if list(trouvailles(texte)) != resultat:
            print(f'TEST INTERNE: cas {numero} — ECHEC')
            return 1
    print(f'TEST INTERNE: {len(cas)} cas — OK')
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    choix = parser.add_mutually_exclusive_group()
    choix.add_argument('--tous', action='store_true', help='tous les fichiers suivis (défaut)')
    choix.add_argument('--chemins', nargs='+', metavar='FICHIER', help='liste explicite de fichiers')
    choix.add_argument('--test-interne', action='store_true', help='vérifications internes factices')
    args = parser.parse_args(argv)

    if args.test_interne:
        return test_interne()

    if args.chemins is not None:
        chemins = list(dict.fromkeys(args.chemins))
    else:
        try:
            resultat = subprocess.run(
                ['git', 'ls-files', '-z'], check=True,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            )
        except (OSError, subprocess.CalledProcessError):
            print('ERREUR: impossible de lister les fichiers suivis par Git.', file=sys.stderr)
            return 2
        chemins = [os.fsdecode(nom) for nom in resultat.stdout.split(b'\0') if nom]

    total = 0
    nb_jwt = 0
    for nom in chemins:
        chemin = Path(nom)
        # Git suit le lien lui-même : ne pas lire sa cible, potentiellement externe.
        if chemin.is_symlink() or chemin.is_dir():
            continue
        try:
            donnees = chemin.read_bytes()
        except OSError:
            print('ERREUR: impossible de lire un fichier demandé.', file=sys.stderr)
            return 2
        if b'\0' in donnees:
            continue
        try:
            texte = donnees.decode('utf-8')
        except UnicodeError:
            continue
        for numero, role, empreinte in trouvailles(texte):
            # Ne jamais inclure la ligne source ni la valeur dans les sorties.
            print(f'{nom}:{numero} role={role} sha256={empreinte}')
            total += 1
            if role != 'non-JWT':
                nb_jwt += 1

    if total:
        print(f'RESULTAT: {total} secret(s) litteral(aux) dont {nb_jwt} JWT — ECHEC')
        return 1
    print('RESULTAT: 0 litteral JWT — OK')
    return 0


if __name__ == '__main__':
    sys.exit(main())
